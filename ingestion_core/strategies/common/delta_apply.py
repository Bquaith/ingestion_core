from __future__ import annotations

from dataclasses import dataclass
import json
from time import perf_counter
from typing import Any, Callable, Mapping, Sequence
import uuid

from sqlalchemy import BigInteger, Column, DateTime, MetaData, Table, Text, text
from sqlalchemy.sql.type_api import TypeEngine

from ingestion_core.adapters.object_store import ObjectStoreClient, ObjectStoreConfig
from ingestion_core.adapters.postgres import (
    create_sqlalchemy_engine,
    ensure_schema,
    parse_table_name,
    reflect_table,
    validate_identifier,
)
from ingestion_core.contracts.runtime import coerce_contract_value, contract_field_nullable, sqlalchemy_type_from_contract_field
from ingestion_core.contracts.types import ContractDefinition
from ingestion_core.strategies.common.change_detection import build_row_key, classify_changes, read_existing_hashes_for_keys
from ingestion_core.strategies.common.target import (
    delete_rows_by_keys,
    ensure_hash_state_table,
    ensure_target_table_from_contract,
    upsert_changed_rows,
)

DELTA_OP_UPSERT = "UPSERT"
DELTA_OP_DELETE = "DELETE"


@dataclass(frozen=True)
class DeltaMetadataColumn:
    name: str
    type_: TypeEngine
    nullable: bool = False


@dataclass(frozen=True)
class ParsedDeltaEvent:
    op: str
    key: dict[str, Any]
    row: dict[str, Any] | None
    row_hash: str | None
    metadata: dict[str, Any]
    position: dict[str, Any] | None = None
    position_sort_key: tuple[Any, ...] | None = None


@dataclass(frozen=True)
class DeltaApplyResult:
    read_count: int
    effective_row_count: int
    insert_count: int
    update_count: int
    delete_count: int
    unchanged_count: int
    processed_batches: int
    load_seconds: float
    diff_seconds: float
    write_seconds: float
    total_seconds: float
    last_position: dict[str, Any] | None

    def to_dict(self) -> dict[str, Any]:
        return {
            "read_count": self.read_count,
            "effective_row_count": self.effective_row_count,
            "insert_count": self.insert_count,
            "update_count": self.update_count,
            "delete_count": self.delete_count,
            "unchanged_count": self.unchanged_count,
            "processed_batches": self.processed_batches,
            "load_seconds": self.load_seconds,
            "diff_seconds": self.diff_seconds,
            "write_seconds": self.write_seconds,
            "total_seconds": self.total_seconds,
            "last_position": self.last_position,
        }


def quote_identifier(name: str) -> str:
    return f'"{validate_identifier(name)}"'


def qualify_table(schema: str, table: str) -> str:
    return f"{quote_identifier(schema)}.{quote_identifier(table)}"


def build_delta_staging_table_name(prefix: str, target_table_name: str) -> str:
    suffix = uuid.uuid4().hex[:10]
    return f"_{validate_identifier(prefix)}_{target_table_name}_{suffix}"[:63]


def _create_delta_staging_table(
    target_engine,
    target_schema: str,
    target_table_name: str,
    contract: ContractDefinition,
    metadata_columns: Sequence[DeltaMetadataColumn],
) -> Table:
    ensure_schema(target_engine, target_schema)
    metadata = MetaData()
    table = Table(
        target_table_name,
        metadata,
        *(Column(column.name, column.type_, nullable=column.nullable) for column in metadata_columns),
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
    metadata_column_names: Sequence[str],
    order_by_desc_sql: Sequence[str],
) -> list[dict[str, Any]]:
    quoted_columns = [
        *(quote_identifier(column) for column in metadata_column_names),
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
                    ORDER BY {', '.join(order_by_desc_sql)}
                ) AS rn
            FROM {staging_ref}
        ) ranked
        WHERE rn = 1
        ORDER BY {', '.join(order_by_desc_sql)}
        """
    )
    with target_engine.connect() as conn:
        return [dict(row) for row in conn.execute(query).mappings().all()]


def apply_delta_artifact_to_curated(
    target_dsn: str,
    target_table_curated: str,
    contract: ContractDefinition,
    object_store_config: ObjectStoreConfig,
    delta_object_key: str,
    parse_event_payload: Callable[[Mapping[str, Any]], ParsedDeltaEvent],
    metadata_columns: Sequence[DeltaMetadataColumn],
    order_by_desc_sql: Sequence[str],
    staging_table_prefix: str,
    load_batch_size: int = 1000,
    upsert_batch_size: int = 1000,
) -> DeltaApplyResult:
    if load_batch_size <= 0:
        raise ValueError("load_batch_size must be greater than zero")
    if upsert_batch_size <= 0:
        raise ValueError("upsert_batch_size must be greater than zero")
    if not metadata_columns:
        raise ValueError("metadata_columns must not be empty")
    if not order_by_desc_sql:
        raise ValueError("order_by_desc_sql must not be empty")

    target_engine = create_sqlalchemy_engine(target_dsn)
    object_store = ObjectStoreClient(object_store_config)
    started_at = perf_counter()
    load_seconds = 0.0
    diff_seconds = 0.0
    write_seconds = 0.0
    read_count = 0
    processed_batches = 0
    latest_position: dict[str, Any] | None = None
    latest_position_sort_key: tuple[Any, ...] | None = None

    try:
        target_schema, target_table_name = parse_table_name(target_table_curated)
        staging_table_name = build_delta_staging_table_name(staging_table_prefix, target_table_name)
        staging_table = _create_delta_staging_table(
            target_engine=target_engine,
            target_schema=target_schema,
            target_table_name=staging_table_name,
            contract=contract,
            metadata_columns=metadata_columns,
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

                    event = parse_event_payload(json.loads(line))
                    read_count += 1
                    if event.position and event.position_sort_key is not None:
                        if latest_position_sort_key is None or event.position_sort_key > latest_position_sort_key:
                            latest_position_sort_key = event.position_sort_key
                            latest_position = dict(event.position)

                    staging_row = {
                        **event.metadata,
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
            effective_rows = _select_latest_effective_rows(
                target_engine=target_engine,
                staging_table=staging_table,
                contract=contract,
                metadata_column_names=[column.name for column in metadata_columns],
                order_by_desc_sql=order_by_desc_sql,
            )
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
            return DeltaApplyResult(
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
                last_position=latest_position,
            )
        finally:
            _drop_staging_table(target_engine=target_engine, staging_table=staging_table)
    finally:
        target_engine.dispose()
