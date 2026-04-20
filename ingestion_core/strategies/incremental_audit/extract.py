from __future__ import annotations

from datetime import datetime
import gzip
import json
import logging
import os
from tempfile import NamedTemporaryFile
from time import perf_counter
from typing import Any, Iterable, Mapping

from sqlalchemy import text

from ingestion_core.adapters.object_store import ObjectStoreClient, ObjectStoreConfig
from ingestion_core.adapters.postgres import create_sqlalchemy_engine, parse_table_name
from ingestion_core.contracts.runtime import (
    ContractValidationError,
    build_contract_key_payload,
    build_contract_row_payload,
    normalize_contract_key_row,
    normalize_contract_row,
    summarize_validation_errors,
)
from ingestion_core.contracts.types import ContractDefinition
from ingestion_core.strategies.incremental_audit.admin import resolve_watermark_mode
from ingestion_core.strategies.incremental_audit.sql import (
    build_select_audit_window_sql,
    build_select_latest_watermark_sql,
)
from ingestion_core.strategies.incremental_audit.types import (
    DELTA_OP_DELETE,
    DELTA_OP_UPSERT,
    AuditWatermark,
    ExtractValidateDeltaResult,
    NormalizedDeltaEvent,
    SourceAuditEvent,
    WATERMARK_MODE_AUTO,
)

logger = logging.getLogger(__name__)

_VALIDATION_ERROR_PREVIEW_LIMIT = 5


def _write_manifest(
    object_store: ObjectStoreClient,
    manifest_key: str,
    payload: Mapping[str, Any],
) -> str:
    return object_store.put_json(manifest_key, dict(payload))


def _select_latest_watermark(
    source_engine,
    audit_schema: str,
    audit_table: str,
    watermark_mode: str,
) -> AuditWatermark | None:
    statement = text(build_select_latest_watermark_sql(audit_schema, audit_table, watermark_mode))
    with source_engine.connect() as conn:
        row = conn.execute(statement).mappings().first()
    if row is None:
        return None
    return AuditWatermark(
        ordering_ts=row["ordering_ts"],
        event_id=int(row["audit_event_id"]),
        mode=watermark_mode,
    )


def _iter_source_audit_event_batches(
    source_engine,
    audit_schema: str,
    audit_table: str,
    watermark_mode: str,
    window_start: AuditWatermark | None,
    window_end: AuditWatermark,
    extract_batch_size: int,
) -> Iterable[tuple[list[SourceAuditEvent], float]]:
    query = text(
        build_select_audit_window_sql(
            audit_schema=audit_schema,
            audit_table=audit_table,
            watermark_mode=watermark_mode,
            has_lower_bound=window_start is not None,
        )
    )
    params: dict[str, Any] = {
        "end_ordering_ts": window_end.ordering_ts,
        "end_event_id": window_end.event_id,
    }
    if window_start is not None:
        params["start_ordering_ts"] = window_start.ordering_ts
        params["start_event_id"] = window_start.event_id

    with source_engine.connect() as conn:
        result = conn.execution_options(stream_results=True).execute(query, params).mappings()

        while True:
            fetch_started = perf_counter()
            rows = result.fetchmany(extract_batch_size)
            fetch_seconds = perf_counter() - fetch_started
            if not rows:
                break
            yield [SourceAuditEvent.from_mapping(row) for row in rows], fetch_seconds


def _event_errors(
    source_event: SourceAuditEvent,
    errors: list[dict[str, Any]],
    event_role: str,
) -> list[dict[str, Any]]:
    normalized: list[dict[str, Any]] = []
    for error in errors:
        payload = dict(error)
        payload["source_event_id"] = source_event.event_id
        payload["source_op"] = source_event.op
        payload["event_ts"] = source_event.ordering_ts.isoformat()
        payload["event_role"] = event_role
        normalized.append(payload)
    return normalized


def _normalize_delete_event(
    source_event: SourceAuditEvent,
    contract: ContractDefinition,
    row_number: int,
) -> tuple[NormalizedDeltaEvent | None, list[dict[str, Any]]]:
    delete_row = source_event.row_before or source_event.key_json
    validation_result = normalize_contract_key_row(delete_row, contract, row_number=row_number)
    if validation_result.errors:
        return None, _event_errors(source_event, validation_result.errors, DELTA_OP_DELETE)

    return (
        NormalizedDeltaEvent(
            event_id=source_event.event_id,
            event_ts=source_event.ordering_ts,
            op=DELTA_OP_DELETE,
            key=build_contract_key_payload(validation_result.normalized_row, contract),
            row=None,
            row_hash=None,
        ),
        [],
    )


def _normalize_upsert_event(
    source_event: SourceAuditEvent,
    contract: ContractDefinition,
    row_number: int,
) -> tuple[NormalizedDeltaEvent | None, list[dict[str, Any]]]:
    if not isinstance(source_event.row_after, Mapping):
        return None, _event_errors(
            source_event,
            [
                {
                    "row_number": row_number,
                    "field": "$",
                    "code": "missing_row_after",
                    "message": "Audit event does not include row_after for upsert processing",
                }
            ],
            DELTA_OP_UPSERT,
        )

    validation_result = normalize_contract_row(source_event.row_after, contract, row_number=row_number)
    if validation_result.errors:
        return None, _event_errors(source_event, validation_result.errors, DELTA_OP_UPSERT)

    payload = build_contract_row_payload(validation_result.normalized_row, contract)
    row_hash = str(payload.pop("row_hash"))
    return (
        NormalizedDeltaEvent(
            event_id=source_event.event_id,
            event_ts=source_event.ordering_ts,
            op=DELTA_OP_UPSERT,
            key={field: payload[field] for field in contract.key_fields},
            row=payload,
            row_hash=row_hash,
        ),
        [],
    )


def _normalize_source_audit_event(
    source_event: SourceAuditEvent,
    contract: ContractDefinition,
    row_number: int,
) -> tuple[list[NormalizedDeltaEvent], list[dict[str, Any]]]:
    normalized_events: list[NormalizedDeltaEvent] = []
    errors: list[dict[str, Any]] = []

    if source_event.op == "I":
        upsert_event, event_errors = _normalize_upsert_event(source_event, contract, row_number)
        if event_errors:
            return [], event_errors
        if upsert_event is not None:
            normalized_events.append(upsert_event)
        return normalized_events, []

    if source_event.op == "D":
        delete_event, event_errors = _normalize_delete_event(source_event, contract, row_number)
        if event_errors:
            return [], event_errors
        if delete_event is not None:
            normalized_events.append(delete_event)
        return normalized_events, []

    if source_event.op == "U":
        old_key_payload = source_event.row_before or source_event.key_json
        old_key_result = normalize_contract_key_row(old_key_payload, contract, row_number=row_number)
        new_row_result = None
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
                            "message": "Audit update event does not include row_after",
                        }
                    ],
                    DELTA_OP_UPSERT,
                )
            )
        if old_key_result.errors:
            errors.extend(_event_errors(source_event, old_key_result.errors, DELTA_OP_DELETE))
        if new_row_result is not None and new_row_result.errors:
            errors.extend(_event_errors(source_event, new_row_result.errors, DELTA_OP_UPSERT))
        if errors:
            return [], errors

        if new_row_result is None:
            return [], errors

        new_key_tuple = tuple(new_row_result.normalized_row[field] for field in contract.key_fields)
        old_key_tuple = tuple(old_key_result.normalized_row[field] for field in contract.key_fields)
        if old_key_tuple != new_key_tuple:
            normalized_events.append(
                NormalizedDeltaEvent(
                    event_id=source_event.event_id,
                    event_ts=source_event.ordering_ts,
                    op=DELTA_OP_DELETE,
                    key=build_contract_key_payload(old_key_result.normalized_row, contract),
                    row=None,
                    row_hash=None,
                )
            )

        upsert_payload = build_contract_row_payload(new_row_result.normalized_row, contract)
        row_hash = str(upsert_payload.pop("row_hash"))
        normalized_events.append(
            NormalizedDeltaEvent(
                event_id=source_event.event_id,
                event_ts=source_event.ordering_ts,
                op=DELTA_OP_UPSERT,
                key={field: upsert_payload[field] for field in contract.key_fields},
                row=upsert_payload,
                row_hash=row_hash,
            )
        )
        return normalized_events, []

    return [], [
        {
            "row_number": row_number,
            "field": "$",
            "code": "unsupported_op",
            "message": f"Unsupported audit operation: {source_event.op}",
            "source_event_id": source_event.event_id,
            "source_op": source_event.op,
            "event_ts": source_event.ordering_ts.isoformat(),
            "event_role": "AUDIT",
        }
    ]


def extract_validate_land_delta(
    source_dsn: str,
    source_audit_table: str,
    contract: ContractDefinition,
    object_store_config: ObjectStoreConfig,
    delta_object_key: str,
    error_object_key: str,
    manifest_key: str,
    extract_batch_size: int = 1000,
    start_watermark: AuditWatermark | None = None,
    watermark_mode: str = WATERMARK_MODE_AUTO,
) -> ExtractValidateDeltaResult:
    if extract_batch_size <= 0:
        raise ValueError("extract_batch_size must be greater than zero")

    source_engine = create_sqlalchemy_engine(source_dsn)
    object_store = ObjectStoreClient(object_store_config)
    effective_watermark_mode = resolve_watermark_mode(source_dsn, watermark_mode)
    source_event_count = 0
    normalized_event_count = 0
    upsert_event_count = 0
    delete_event_count = 0
    invalid_event_count = 0
    processed_batches = 0
    source_read_seconds = 0.0
    event_row_number = 0
    preview_errors: list[dict[str, Any]] = []

    with NamedTemporaryFile(suffix=".accepted-delta.ndjson.gz", delete=False) as delta_temp_file:
        delta_temp_path = delta_temp_file.name
    with NamedTemporaryFile(suffix=".errors.ndjson.gz", delete=False) as error_temp_file:
        error_temp_path = error_temp_file.name

    try:
        audit_schema, audit_table = parse_table_name(source_audit_table)
        window_end = _select_latest_watermark(
            source_engine=source_engine,
            audit_schema=audit_schema,
            audit_table=audit_table,
            watermark_mode=effective_watermark_mode,
        )

        logger.info(
            "Extract-validate-land-delta started for contract_id=%s contract_version=%s source_audit_table=%s",
            contract.contract_id,
            contract.version,
            source_audit_table,
        )

        with gzip.open(delta_temp_path, mode="wt", encoding="utf-8") as delta_writer, gzip.open(
            error_temp_path,
            mode="wt",
            encoding="utf-8",
        ) as error_writer:
            if window_end is not None:
                for source_batch, fetch_seconds in _iter_source_audit_event_batches(
                    source_engine=source_engine,
                    audit_schema=audit_schema,
                    audit_table=audit_table,
                    watermark_mode=effective_watermark_mode,
                    window_start=start_watermark,
                    window_end=window_end,
                    extract_batch_size=extract_batch_size,
                ):
                    processed_batches += 1
                    source_read_seconds += fetch_seconds
                    source_event_count += len(source_batch)

                    for source_event in source_batch:
                        event_row_number += 1
                        normalized_events, event_errors = _normalize_source_audit_event(
                            source_event,
                            contract,
                            row_number=event_row_number,
                        )
                        if event_errors:
                            invalid_event_count += 1
                            if len(preview_errors) < _VALIDATION_ERROR_PREVIEW_LIMIT:
                                remaining_slots = _VALIDATION_ERROR_PREVIEW_LIMIT - len(preview_errors)
                                preview_errors.extend(dict(error) for error in event_errors[:remaining_slots])
                            for error in event_errors:
                                error_writer.write(json.dumps(error, ensure_ascii=True))
                                error_writer.write("\n")
                            continue

                        for normalized_event in normalized_events:
                            payload = normalized_event.to_payload()
                            delta_writer.write(json.dumps(payload, ensure_ascii=True))
                            delta_writer.write("\n")
                            normalized_event_count += 1
                            if normalized_event.op == DELTA_OP_UPSERT:
                                upsert_event_count += 1
                            else:
                                delete_event_count += 1

        uploaded_error_key = object_store.upload_file(
            error_temp_path,
            error_object_key,
            content_type="application/x-ndjson",
            content_encoding="gzip",
        )
        uploaded_delta_key: str | None = None
        if invalid_event_count == 0:
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
                "stage": "extract_validate_land_delta",
                "source_audit_table": source_audit_table,
                "source_event_count": source_event_count,
                "normalized_event_count": normalized_event_count,
                "upsert_event_count": upsert_event_count,
                "delete_event_count": delete_event_count,
                "invalid_event_count": invalid_event_count,
                "processed_batches": processed_batches,
                "source_read_seconds": source_read_seconds,
                "contract_id": contract.contract_id,
                "contract_version": contract.version,
                "delta_object_key": uploaded_delta_key,
                "error_object_key": uploaded_error_key,
                "window_start": start_watermark.to_dict() if start_watermark else None,
                "window_end": window_end.to_dict() if window_end else None,
                "watermark_mode": effective_watermark_mode,
                "sample_errors": preview_errors,
            },
        )

        if invalid_event_count > 0:
            error_summary = summarize_validation_errors(preview_errors)
            raise ContractValidationError(
                "Validation failed for contract "
                f"{contract.contract_id}: {invalid_event_count} invalid audit events. "
                f"Error report: {uploaded_error_key}. "
                f"Examples: {error_summary}"
            )

        return ExtractValidateDeltaResult(
            delta_object_key=uploaded_delta_key,
            error_object_key=uploaded_error_key,
            manifest_key=manifest_object_key,
            source_event_count=source_event_count,
            normalized_event_count=normalized_event_count,
            upsert_event_count=upsert_event_count,
            delete_event_count=delete_event_count,
            invalid_event_count=invalid_event_count,
            processed_batches=processed_batches,
            source_read_seconds=source_read_seconds,
            window_start=start_watermark,
            window_end=window_end,
            watermark_mode=effective_watermark_mode,
        )
    finally:
        source_engine.dispose()
        if os.path.exists(delta_temp_path):
            os.unlink(delta_temp_path)
        if os.path.exists(error_temp_path):
            os.unlink(error_temp_path)
