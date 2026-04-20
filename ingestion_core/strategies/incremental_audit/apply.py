from __future__ import annotations

import json
from time import perf_counter
from typing import Any
import uuid

from sqlalchemy import BigInteger, Column, DateTime, MetaData, Table, Text, text

from ingestion_core.adapters.object_store import ObjectStoreClient, ObjectStoreConfig
from ingestion_core.adapters.postgres import create_sqlalchemy_engine, ensure_schema, parse_table_name, reflect_table
from ingestion_core.contracts.runtime import coerce_contract_value, contract_field_nullable, sqlalchemy_type_from_contract_field
from ingestion_core.contracts.types import ContractDefinition
from ingestion_core.strategies.common import (
    build_row_key,
    classify_changes,
    delete_rows_by_keys,
    ensure_hash_state_table,
    ensure_target_table_from_contract,
    read_existing_hashes_for_keys,
    upsert_changed_rows,
)
from ingestion_core.strategies.common.change_detection import chunk_rows
from ingestion_core.strategies.incremental_audit.sql import qualify_table, quote_identifier
from ingestion_core.strategies.incremental_audit.types import (
    ApplyDeltaResult,
    AuditWatermark,
    DELTA_OP_DELETE,
    DELTA_OP_UPSERT,
    NormalizedDeltaEvent,
)


def _build_delta_staging_table_name(target_table_name: str) -> str:
    suffix = uuid.uuid4().hex[:10]
    return f"_ia_delta_{target_table_name}_{suffix}"[:63]


def _create_delta_staging_table(
    target_engine,
    target_schema: str,
    target_table_name: str,
    contract: ContractDefinition,
) -> Table:
    ensure_schema(target_engine, target_schema)
    metadata = MetaData()
    table = Table(
        target_table_name,
        metadata,
        Column("event_id", BigInteger, nullable=False),
        Column("event_ts", DateTime(timezone=True), nullable=False),
        Column("op", Text, nullable=False),
        *(
            Column(
                field,
                sqlalchemy_type_from_contract_field(contract, field),
                nullable=True if field not in contract.key_fields else contract_field_nullable(contract, field),
            )
            for field in contract.fields
        ),
        Column("row_hash", Text, nullable=True),
        schema=target_schema,
    )
    metadata.create_all(target_engine, tables=[table], checkfirst=False)
    return reflect_table(target_engine, target_schema, target_table_name)


def _drop_staging_table(target_engine, staging_table: Table) -> None:
    staging_table.drop(target_engine, checkfirst=True)


def _select_latest_effective_rows(
    target_engine,
    staging_table: Table,
    contract: ContractDefinition,
) -> list[dict[str, Any]]:
    quoted_columns = [
        quote_identifier("event_id"),
        quote_identifier("event_ts"),
        quote_identifier("op"),
        quote_identifier("row_hash"),
        *(quote_identifier(field) for field in contract.fields),
    ]
    partition_by = ", ".join(quote_identifier(field) for field in contract.key_fields)
    staging_ref = qualify_table(staging_table.schema or "public", staging_table.name)
    query = text(
        f"""
        SELECT {', '.join(quoted_columns)}
        FROM (
            SELECT
                {', '.join(quoted_columns)},
                ROW_NUMBER() OVER (
                    PARTITION BY {partition_by}
                    ORDER BY {quote_identifier('event_ts')} DESC, {quote_identifier('event_id')} DESC
                ) AS rn
            FROM {staging_ref}
        ) ranked
        WHERE rn = 1
        ORDER BY {quote_identifier('event_ts')} ASC, {quote_identifier('event_id')} ASC
        """
    )
    with target_engine.connect() as conn:
        return [dict(row) for row in conn.execute(query).mappings().all()]


def apply_delta_to_curated(
    target_dsn: str,
    target_table_curated: str,
    contract: ContractDefinition,
    object_store_config: ObjectStoreConfig,
    delta_object_key: str,
    load_batch_size: int = 1000,
    upsert_batch_size: int = 1000,
) -> ApplyDeltaResult:
    if load_batch_size <= 0:
        raise ValueError("load_batch_size must be greater than zero")
    if upsert_batch_size <= 0:
        raise ValueError("upsert_batch_size must be greater than zero")

    target_engine = create_sqlalchemy_engine(target_dsn)
    object_store = ObjectStoreClient(object_store_config)
    started_at = perf_counter()
    load_seconds = 0.0
    diff_seconds = 0.0
    write_seconds = 0.0
    read_count = 0
    processed_batches = 0
    latest_watermark: AuditWatermark | None = None

    try:
        target_schema, target_table_name = parse_table_name(target_table_curated)
        staging_table_name = _build_delta_staging_table_name(target_table_name)
        staging_table = _create_delta_staging_table(
            target_engine=target_engine,
            target_schema=target_schema,
            target_table_name=staging_table_name,
            contract=contract,
        )
        staging_table.metadata.bind = target_engine

        try:
            load_started = perf_counter()
            with target_engine.begin() as conn, object_store.open_gzip_text_reader(delta_object_key) as reader:
                rows_buffer: list[dict[str, Any]] = []
                for raw_line in reader:
                    line = raw_line.strip()
                    if not line:
                        continue

                    event = NormalizedDeltaEvent.from_payload(json.loads(line))
                    read_count += 1
                    candidate_watermark = AuditWatermark(
                        ordering_ts=event.event_ts,
                        event_id=event.event_id,
                        mode="event_ts",
                    )
                    if latest_watermark is None or (
                        event.event_ts > latest_watermark.ordering_ts
                        or (event.event_ts == latest_watermark.ordering_ts and event.event_id > latest_watermark.event_id)
                    ):
                        latest_watermark = candidate_watermark

                    staging_row = {
                        "event_id": event.event_id,
                        "event_ts": event.event_ts,
                        "op": event.op,
                        "row_hash": event.row_hash,
                    }
                    row_payload = event.row or {}
                    key_payload = event.key
                    for field in contract.fields:
                        raw_value = row_payload.get(field, key_payload.get(field))
                        if event.op == DELTA_OP_DELETE and field not in contract.key_fields:
                            staging_row[field] = None
                        else:
                            staging_row[field] = coerce_contract_value(raw_value, contract.field_types.get(field))
                    rows_buffer.append(staging_row)

                    if len(rows_buffer) >= load_batch_size:
                        conn.execute(staging_table.insert(), rows_buffer)
                        processed_batches += 1
                        rows_buffer = []

                if rows_buffer:
                    conn.execute(staging_table.insert(), rows_buffer)
                    processed_batches += 1
            load_seconds = perf_counter() - load_started

            target_table = ensure_target_table_from_contract(
                target_engine=target_engine,
                target_schema=target_schema,
                target_table_name=target_table_name,
                contract=contract,
            )
            hash_state_table = ensure_hash_state_table(
                target_engine=target_engine,
                target_schema=target_schema,
                target_table_name=target_table_name,
                target_table=target_table,
                key_fields=contract.key_fields,
            )

            diff_started = perf_counter()
            effective_rows = _select_latest_effective_rows(target_engine, staging_table, contract)
            diff_seconds = perf_counter() - diff_started

            delete_keys: list[tuple[Any, ...]] = []
            upsert_rows: list[dict[str, Any]] = []
            for row in effective_rows:
                if row["op"] == DELTA_OP_DELETE:
                    delete_keys.append(tuple(row[field] for field in contract.key_fields))
                    continue
                row_payload = {field: row[field] for field in contract.fields}
                row_payload["row_hash"] = str(row["row_hash"])
                upsert_rows.append(row_payload)

            existing_hashes = read_existing_hashes_for_keys(
                target_engine=target_engine,
                hash_state_table=hash_state_table,
                key_fields=contract.key_fields,
                key_tuples=[build_row_key(row, contract.key_fields) for row in upsert_rows],
            )
            inserts, updates, unchanged_count = classify_changes(
                source_rows=upsert_rows,
                existing_hashes=existing_hashes,
                key_fields=contract.key_fields,
            )

            write_started = perf_counter()
            upsert_changed_rows(
                target_engine=target_engine,
                target_table=target_table,
                hash_state_table=hash_state_table,
                rows=[*inserts, *updates],
                key_fields=contract.key_fields,
                fields=contract.fields,
                upsert_batch_size=upsert_batch_size,
            )
            delete_count = delete_rows_by_keys(
                target_engine=target_engine,
                target_table=target_table,
                hash_state_table=hash_state_table,
                key_fields=contract.key_fields,
                key_tuples=delete_keys,
                batch_size=upsert_batch_size,
            )
            write_seconds = perf_counter() - write_started

            total_seconds = perf_counter() - started_at
            return ApplyDeltaResult(
                read_count=read_count,
                effective_row_count=len(effective_rows),
                insert_count=len(inserts),
                update_count=len(updates),
                delete_count=delete_count,
                unchanged_count=unchanged_count,
                processed_batches=processed_batches,
                load_seconds=load_seconds,
                diff_seconds=diff_seconds,
                write_seconds=write_seconds,
                total_seconds=total_seconds,
                last_applied_watermark=latest_watermark,
            )
        finally:
            _drop_staging_table(target_engine=target_engine, staging_table=staging_table)
    finally:
        target_engine.dispose()
