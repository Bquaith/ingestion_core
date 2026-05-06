from __future__ import annotations

from datetime import datetime, timezone
import gzip
import json
import logging
import os
import select
from tempfile import NamedTemporaryFile
from time import perf_counter
from typing import Any, Mapping

from sqlalchemy import text

from ingestion_core.adapters.object_store import ObjectStoreClient, ObjectStoreConfig
from ingestion_core.adapters.postgres import create_sqlalchemy_engine
from ingestion_core.contracts.runtime import (
    build_contract_key_payload,
    build_contract_row_payload,
    normalize_contract_key_row,
    normalize_contract_row,
    summarize_validation_errors,
)
from ingestion_core.contracts.types import ContractDefinition
from ingestion_core.strategies.logical_cdc.decode import PgOutputDecoder
from ingestion_core.strategies.logical_cdc.sql import build_current_wal_lsn_sql
from ingestion_core.strategies.logical_cdc.types import (
    CDC_OP_DELETE,
    CDC_OP_UPSERT,
    OUTPUT_PLUGIN_PGOUTPUT,
    ExtractValidateLogicalCdcResult,
    LogicalCdcDeltaEvent,
    LogicalCdcSourceEvent,
    int_to_lsn,
    lsn_to_int,
)

logger = logging.getLogger(__name__)

_VALIDATION_ERROR_PREVIEW_LIMIT = 5


class _StopReplication(Exception):
    pass


def _transaction_identity(source_event: LogicalCdcSourceEvent) -> tuple[str, int | None]:
    return source_event.commit_lsn, source_event.xid


def _write_manifest(
    object_store: ObjectStoreClient,
    manifest_key: str,
    payload: Mapping[str, Any],
) -> str:
    return object_store.put_json(manifest_key, dict(payload))


def _select_current_wal_lsn(source_dsn: str) -> str:
    engine = create_sqlalchemy_engine(source_dsn)
    try:
        with engine.connect() as conn:
            return str(conn.execute(text(build_current_wal_lsn_sql())).scalar_one())
    finally:
        engine.dispose()


def _event_errors(
    source_event: LogicalCdcSourceEvent,
    errors: list[dict[str, Any]],
    event_role: str,
) -> list[dict[str, Any]]:
    normalized: list[dict[str, Any]] = []
    for error in errors:
        payload = dict(error)
        payload["source_op"] = source_event.source_op
        payload["commit_lsn"] = source_event.commit_lsn
        payload["change_index"] = source_event.change_index
        payload["event_role"] = event_role
        normalized.append(payload)
    return normalized


def _extend_preview_errors(preview_errors: list[dict[str, Any]], errors: list[dict[str, Any]]) -> None:
    if len(preview_errors) >= _VALIDATION_ERROR_PREVIEW_LIMIT:
        return
    remaining_slots = _VALIDATION_ERROR_PREVIEW_LIMIT - len(preview_errors)
    preview_errors.extend(dict(error) for error in errors[:remaining_slots])


def _normalize_delete_event(
    source_event: LogicalCdcSourceEvent,
    contract: ContractDefinition,
    row_number: int,
) -> tuple[LogicalCdcDeltaEvent | None, list[dict[str, Any]]]:
    delete_row = source_event.old_key or {}
    validation_result = normalize_contract_key_row(delete_row, contract, row_number=row_number)
    if validation_result.errors:
        return None, _event_errors(source_event, validation_result.errors, CDC_OP_DELETE)

    return (
        LogicalCdcDeltaEvent(
            commit_lsn=source_event.commit_lsn,
            end_lsn=source_event.end_lsn,
            change_index=source_event.change_index,
            op=CDC_OP_DELETE,
            key=build_contract_key_payload(validation_result.normalized_row, contract),
            row=None,
            row_hash=None,
            xid=source_event.xid,
            commit_ts=source_event.commit_ts,
        ),
        [],
    )


def _normalize_upsert_event(
    source_event: LogicalCdcSourceEvent,
    contract: ContractDefinition,
    row_number: int,
) -> tuple[LogicalCdcDeltaEvent | None, list[dict[str, Any]]]:
    if not isinstance(source_event.row_after, Mapping):
        return None, _event_errors(
            source_event,
            [
                {
                    "row_number": row_number,
                    "field": "$",
                    "code": "missing_row_after",
                    "message": "WAL event does not include row_after for upsert processing",
                }
            ],
            CDC_OP_UPSERT,
        )

    validation_result = normalize_contract_row(source_event.row_after, contract, row_number=row_number)
    if validation_result.errors:
        return None, _event_errors(source_event, validation_result.errors, CDC_OP_UPSERT)

    payload = build_contract_row_payload(validation_result.normalized_row, contract)
    row_hash = str(payload.pop("row_hash"))
    return (
        LogicalCdcDeltaEvent(
            commit_lsn=source_event.commit_lsn,
            end_lsn=source_event.end_lsn,
            change_index=source_event.change_index,
            op=CDC_OP_UPSERT,
            key={field: payload[field] for field in contract.key_fields},
            row=payload,
            row_hash=row_hash,
            xid=source_event.xid,
            commit_ts=source_event.commit_ts,
        ),
        [],
    )


def _normalize_source_event(
    source_event: LogicalCdcSourceEvent,
    contract: ContractDefinition,
    row_number: int,
) -> tuple[list[LogicalCdcDeltaEvent], list[dict[str, Any]]]:
    if source_event.source_op == "I":
        event, errors = _normalize_upsert_event(source_event, contract, row_number)
        return ([event] if event else []), errors

    if source_event.source_op == "D":
        event, errors = _normalize_delete_event(source_event, contract, row_number)
        return ([event] if event else []), errors

    if source_event.source_op == "U":
        new_row_result = None
        errors: list[dict[str, Any]] = []
        if isinstance(source_event.row_after, Mapping):
            new_row_result = normalize_contract_row(source_event.row_after, contract, row_number=row_number)
        else:
            errors.extend(
                _event_errors(
                    source_event,
                    [
                        {
                            "row_number": row_number,
                            "field": "$",
                            "code": "missing_row_after",
                            "message": "WAL update event does not include row_after",
                        }
                    ],
                    CDC_OP_UPSERT,
                )
            )
        if new_row_result is not None and new_row_result.errors:
            errors.extend(_event_errors(source_event, new_row_result.errors, CDC_OP_UPSERT))
        if errors or new_row_result is None:
            return [], errors

        if source_event.old_key is not None:
            old_key_result = normalize_contract_key_row(source_event.old_key, contract, row_number=row_number)
            if old_key_result.errors:
                return [], _event_errors(source_event, old_key_result.errors, CDC_OP_DELETE)
            old_key_row = old_key_result.normalized_row
        else:
            old_key_row = new_row_result.normalized_row

        normalized_events: list[LogicalCdcDeltaEvent] = []
        old_key_tuple = tuple(old_key_row[field] for field in contract.key_fields)
        new_key_tuple = tuple(new_row_result.normalized_row[field] for field in contract.key_fields)
        if old_key_tuple != new_key_tuple:
            normalized_events.append(
                LogicalCdcDeltaEvent(
                    commit_lsn=source_event.commit_lsn,
                    end_lsn=source_event.end_lsn,
                    change_index=source_event.change_index,
                    op=CDC_OP_DELETE,
                    key=build_contract_key_payload(old_key_row, contract),
                    row=None,
                    row_hash=None,
                    xid=source_event.xid,
                    commit_ts=source_event.commit_ts,
                )
            )

        upsert_payload = build_contract_row_payload(new_row_result.normalized_row, contract)
        row_hash = str(upsert_payload.pop("row_hash"))
        normalized_events.append(
            LogicalCdcDeltaEvent(
                commit_lsn=source_event.commit_lsn,
                end_lsn=source_event.end_lsn,
                change_index=source_event.change_index,
                op=CDC_OP_UPSERT,
                key={field: upsert_payload[field] for field in contract.key_fields},
                row=upsert_payload,
                row_hash=row_hash,
                xid=source_event.xid,
                commit_ts=source_event.commit_ts,
            )
        )
        return normalized_events, []

    return [], [
        {
            "row_number": row_number,
            "field": "$",
            "code": "unsupported_op",
            "message": f"Unsupported WAL operation: {source_event.source_op}",
            "commit_lsn": source_event.commit_lsn,
            "change_index": source_event.change_index,
            "event_role": "WAL",
        }
    ]


def _flush_transaction(
    transaction_entries: list[tuple[list[LogicalCdcDeltaEvent], list[dict[str, Any]]]],
    transaction_event_count: int,
    *,
    delta_writer,
    error_writer,
    preview_errors: list[dict[str, Any]],
) -> dict[str, int]:
    has_errors = any(event_errors for _, event_errors in transaction_entries)
    if has_errors:
        invalid_event_count = 0
        for _, event_errors in transaction_entries:
            if not event_errors:
                continue
            invalid_event_count += 1
            _extend_preview_errors(preview_errors, event_errors)
            for error in event_errors:
                error_writer.write(json.dumps(error, ensure_ascii=True))
                error_writer.write("\n")
        return {
            "normalized_event_count": 0,
            "upsert_event_count": 0,
            "delete_event_count": 0,
            "invalid_event_count": invalid_event_count,
            "invalid_transaction_count": 1,
            "quarantined_event_count": transaction_event_count,
            "quarantined_transaction_count": 1,
        }

    normalized_event_count = 0
    upsert_event_count = 0
    delete_event_count = 0
    for normalized_events, _ in transaction_entries:
        for normalized_event in normalized_events:
            delta_writer.write(json.dumps(normalized_event.to_payload(), ensure_ascii=True))
            delta_writer.write("\n")
            normalized_event_count += 1
            if normalized_event.op == CDC_OP_UPSERT:
                upsert_event_count += 1
            else:
                delete_event_count += 1
    return {
        "normalized_event_count": normalized_event_count,
        "upsert_event_count": upsert_event_count,
        "delete_event_count": delete_event_count,
        "invalid_event_count": 0,
        "invalid_transaction_count": 0,
        "quarantined_event_count": 0,
        "quarantined_transaction_count": 0,
    }


def _read_pgoutput_events(
    source_replication_dsn: str,
    source_slot_name: str,
    source_publication_name: str,
    source_table: str,
    window_start_lsn: str | None,
    window_end_lsn: str,
    max_extract_seconds: int,
    idle_timeout_seconds: int | None = None,
) -> tuple[list[LogicalCdcSourceEvent], int, float, str | None, str | None, bool]:
    import psycopg2
    from psycopg2.extras import LogicalReplicationConnection

    decoder = PgOutputDecoder(source_table=source_table)
    events: list[LogicalCdcSourceEvent] = []
    processed_messages = 0
    last_decoded_lsn: str | None = None
    last_stream_wal_end_lsn: str | None = None
    reached_window_end = False
    window_end_int = lsn_to_int(window_end_lsn)
    started_at = perf_counter()
    last_stream_progress_at: float | None = None

    conn = psycopg2.connect(source_replication_dsn, connection_factory=LogicalReplicationConnection)
    try:
        cur = conn.cursor()

        def _refresh_stream_progress(message=None) -> None:
            nonlocal last_stream_wal_end_lsn, reached_window_end, last_stream_progress_at

            wal_end_candidates: list[int] = []
            cursor_wal_end = getattr(cur, "wal_end", None)
            if isinstance(cursor_wal_end, int) and cursor_wal_end > 0:
                wal_end_candidates.append(cursor_wal_end)

            message_wal_end = getattr(message, "wal_end", None)
            if isinstance(message_wal_end, int) and message_wal_end > 0:
                wal_end_candidates.append(message_wal_end)

            if not wal_end_candidates:
                return

            wal_end_int = max(wal_end_candidates)
            previous_wal_end_int = lsn_to_int(last_stream_wal_end_lsn) if last_stream_wal_end_lsn else None
            if previous_wal_end_int is None or wal_end_int > previous_wal_end_int:
                last_stream_wal_end_lsn = int_to_lsn(wal_end_int)
                last_stream_progress_at = perf_counter()
            elif message is not None:
                last_stream_progress_at = perf_counter()

            if wal_end_int >= window_end_int:
                reached_window_end = True

        def _consume(message) -> None:
            nonlocal processed_messages, last_decoded_lsn
            _refresh_stream_progress(message)
            if perf_counter() - started_at > max_extract_seconds:
                raise _StopReplication()

            payload = bytes(message.payload)
            if not payload:
                if reached_window_end:
                    raise _StopReplication()
                return

            processed_messages += 1
            decoded_events = decoder.decode_message(payload)
            for event in decoded_events:
                if lsn_to_int(event.commit_lsn) > window_end_int:
                    raise _StopReplication()
                events.append(event)
                last_decoded_lsn = event.end_lsn

        start_lsn = window_start_lsn or "0/0"
        cur.start_replication(
            slot_name=source_slot_name,
            start_lsn=start_lsn,
            options={
                "proto_version": "1",
                "publication_names": source_publication_name,
                "messages": "false",
            },
            decode=False,
        )
        try:
            while perf_counter() - started_at <= max_extract_seconds:
                message = cur.read_message()
                _refresh_stream_progress()
                if reached_window_end and message is None:
                    break
                if message is None:
                    if (
                        idle_timeout_seconds is not None
                        and last_stream_progress_at is not None
                        and perf_counter() - last_stream_progress_at >= idle_timeout_seconds
                    ):
                        break
                    remaining = max_extract_seconds - (perf_counter() - started_at)
                    if remaining <= 0:
                        break
                    wait_seconds = min(1.0, remaining)
                    if idle_timeout_seconds is not None and last_stream_progress_at is not None:
                        idle_remaining = idle_timeout_seconds - (perf_counter() - last_stream_progress_at)
                        if idle_remaining <= 0:
                            break
                        wait_seconds = min(wait_seconds, idle_remaining)
                    select.select([conn], [], [], wait_seconds)
                    continue
                try:
                    _consume(message)
                except _StopReplication:
                    break
        finally:
            cur.close()
    finally:
        conn.close()

    return (
        events,
        processed_messages,
        perf_counter() - started_at,
        last_decoded_lsn,
        last_stream_wal_end_lsn,
        reached_window_end,
    )


def extract_validate_land_wal_delta(
    source_dsn: str,
    source_replication_dsn: str,
    source_table: str,
    source_slot_name: str,
    source_publication_name: str,
    contract: ContractDefinition,
    object_store_config: ObjectStoreConfig,
    delta_object_key: str,
    error_object_key: str,
    manifest_key: str,
    start_lsn: str | None = None,
    window_end_lsn: str | None = None,
    max_extract_seconds: int = 30,
    idle_timeout_seconds: int | None = None,
    output_plugin: str = OUTPUT_PLUGIN_PGOUTPUT,
) -> ExtractValidateLogicalCdcResult:
    if output_plugin != OUTPUT_PLUGIN_PGOUTPUT:
        raise ValueError("Only native PostgreSQL pgoutput is supported; wal2json is intentionally unsupported")
    if max_extract_seconds <= 0:
        raise ValueError("max_extract_seconds must be greater than zero")
    if idle_timeout_seconds is not None and idle_timeout_seconds <= 0:
        raise ValueError("idle_timeout_seconds must be greater than zero")

    object_store = ObjectStoreClient(object_store_config)
    if window_end_lsn is None:
        window_end_lsn = _select_current_wal_lsn(source_dsn)

    source_event_count = 0
    normalized_event_count = 0
    upsert_event_count = 0
    delete_event_count = 0
    invalid_event_count = 0
    invalid_transaction_count = 0
    quarantined_event_count = 0
    quarantined_transaction_count = 0
    event_row_number = 0
    preview_errors: list[dict[str, Any]] = []

    with NamedTemporaryFile(suffix=".wal-delta.ndjson.gz", delete=False) as delta_temp_file:
        delta_temp_path = delta_temp_file.name
    with NamedTemporaryFile(suffix=".wal-errors.ndjson.gz", delete=False) as error_temp_file:
        error_temp_path = error_temp_file.name

    try:
        (
            source_events,
            processed_messages,
            source_read_seconds,
            last_decoded_lsn,
            last_stream_wal_end_lsn,
            reached_window_end,
        ) = _read_pgoutput_events(
            source_replication_dsn=source_replication_dsn,
            source_slot_name=source_slot_name,
            source_publication_name=source_publication_name,
            source_table=source_table,
            window_start_lsn=start_lsn,
            window_end_lsn=window_end_lsn,
            max_extract_seconds=max_extract_seconds,
            idle_timeout_seconds=idle_timeout_seconds,
        )
        source_event_count = len(source_events)
        if processed_messages == 0 and last_decoded_lsn is None:
            logger.warning(
                "Logical CDC extract read no pgoutput messages for source_table=%s slot=%s "
                "within window_start_lsn=%s window_end_lsn=%s reached_window_end=%s "
                "last_stream_wal_end_lsn=%s",
                source_table,
                source_slot_name,
                start_lsn,
                window_end_lsn,
                reached_window_end,
                last_stream_wal_end_lsn,
            )

        with gzip.open(delta_temp_path, mode="wt", encoding="utf-8") as delta_writer, gzip.open(
            error_temp_path,
            mode="wt",
            encoding="utf-8",
        ) as error_writer:
            current_transaction_identity: tuple[str, int | None] | None = None
            current_transaction_entries: list[tuple[list[LogicalCdcDeltaEvent], list[dict[str, Any]]]] = []
            current_transaction_event_count = 0

            for source_event in source_events:
                transaction_identity = _transaction_identity(source_event)
                if current_transaction_identity is not None and transaction_identity != current_transaction_identity:
                    transaction_counts = _flush_transaction(
                        current_transaction_entries,
                        current_transaction_event_count,
                        delta_writer=delta_writer,
                        error_writer=error_writer,
                        preview_errors=preview_errors,
                    )
                    normalized_event_count += transaction_counts["normalized_event_count"]
                    upsert_event_count += transaction_counts["upsert_event_count"]
                    delete_event_count += transaction_counts["delete_event_count"]
                    invalid_event_count += transaction_counts["invalid_event_count"]
                    invalid_transaction_count += transaction_counts["invalid_transaction_count"]
                    quarantined_event_count += transaction_counts["quarantined_event_count"]
                    quarantined_transaction_count += transaction_counts["quarantined_transaction_count"]
                    current_transaction_entries = []
                    current_transaction_event_count = 0

                current_transaction_identity = transaction_identity
                event_row_number += 1
                normalized_events, event_errors = _normalize_source_event(source_event, contract, event_row_number)
                current_transaction_entries.append((normalized_events, event_errors))
                current_transaction_event_count += 1

            if current_transaction_entries:
                transaction_counts = _flush_transaction(
                    current_transaction_entries,
                    current_transaction_event_count,
                    delta_writer=delta_writer,
                    error_writer=error_writer,
                    preview_errors=preview_errors,
                )
                normalized_event_count += transaction_counts["normalized_event_count"]
                upsert_event_count += transaction_counts["upsert_event_count"]
                delete_event_count += transaction_counts["delete_event_count"]
                invalid_event_count += transaction_counts["invalid_event_count"]
                invalid_transaction_count += transaction_counts["invalid_transaction_count"]
                quarantined_event_count += transaction_counts["quarantined_event_count"]
                quarantined_transaction_count += transaction_counts["quarantined_transaction_count"]

        uploaded_error_key = object_store.upload_file(
            error_temp_path,
            error_object_key,
            content_type="application/x-ndjson",
            content_encoding="gzip",
        )
        uploaded_delta_key = object_store.upload_file(
            delta_temp_path,
            delta_object_key,
            content_type="application/x-ndjson",
            content_encoding="gzip",
        )

        manifest_object_key = _write_manifest(
            object_store,
            manifest_key,
            {
                "strategy": "logical_cdc",
                "stage": "extract_validate_land_wal_delta",
                "source_table": source_table,
                "source_slot_name": source_slot_name,
                "source_publication_name": source_publication_name,
                "window_start_lsn": start_lsn,
                "window_end_lsn": window_end_lsn,
                "last_decoded_lsn": last_decoded_lsn,
                "last_stream_wal_end_lsn": last_stream_wal_end_lsn,
                "reached_window_end": reached_window_end,
                "contract_id": contract.contract_id,
                "contract_version": contract.version,
                "source_event_count": source_event_count,
                "normalized_event_count": normalized_event_count,
                "upsert_event_count": upsert_event_count,
                "delete_event_count": delete_event_count,
                "invalid_event_count": invalid_event_count,
                "invalid_transaction_count": invalid_transaction_count,
                "quarantined_event_count": quarantined_event_count,
                "quarantined_transaction_count": quarantined_transaction_count,
                "processed_messages": processed_messages,
                "source_read_seconds": source_read_seconds,
                "delta_object_key": uploaded_delta_key,
                "error_object_key": uploaded_error_key,
                "output_plugin": output_plugin,
                "sample_errors": preview_errors,
                "extracted_at": datetime.now(timezone.utc).isoformat(),
            },
        )

        if invalid_event_count > 0:
            error_summary = summarize_validation_errors(preview_errors)
            logger.warning(
                "Extract-validate-land-wal-delta completed with quarantined invalid WAL transactions "
                "for contract_id=%s invalid_event_count=%s invalid_transaction_count=%s "
                "quarantined_event_count=%s error_object_key=%s examples=%s",
                contract.contract_id,
                invalid_event_count,
                invalid_transaction_count,
                quarantined_event_count,
                uploaded_error_key,
                error_summary,
            )

        return ExtractValidateLogicalCdcResult(
            delta_object_key=uploaded_delta_key,
            error_object_key=uploaded_error_key,
            manifest_key=manifest_object_key,
            source_event_count=source_event_count,
            normalized_event_count=normalized_event_count,
            upsert_event_count=upsert_event_count,
            delete_event_count=delete_event_count,
            invalid_event_count=invalid_event_count,
            invalid_transaction_count=invalid_transaction_count,
            quarantined_event_count=quarantined_event_count,
            quarantined_transaction_count=quarantined_transaction_count,
            processed_messages=processed_messages,
            source_read_seconds=source_read_seconds,
            window_start_lsn=start_lsn,
            window_end_lsn=window_end_lsn,
            last_decoded_lsn=last_decoded_lsn,
            last_stream_wal_end_lsn=last_stream_wal_end_lsn,
            reached_window_end=reached_window_end,
            output_plugin=output_plugin,
        )
    finally:
        if os.path.exists(delta_temp_path):
            os.unlink(delta_temp_path)
        if os.path.exists(error_temp_path):
            os.unlink(error_temp_path)
