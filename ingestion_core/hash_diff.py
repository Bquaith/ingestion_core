from __future__ import annotations

import uuid
from dataclasses import dataclass
from time import perf_counter
from typing import Any, Iterable, Mapping, Sequence

from sqlalchemy import Column, MetaData, PrimaryKeyConstraint, Table, Text, and_, exists, select, text, tuple_
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.sql.sqltypes import Boolean, Date, DateTime, Integer, JSON, LargeBinary, Numeric, String, Time

from ingestion_core.hashing import calculate_row_hash
from ingestion_core.postgres import (
    create_sqlalchemy_engine,
    ensure_schema,
    parse_table_name,
    reflect_table,
    table_exists,
)


@dataclass(frozen=True)
class ContractDefinition:
    contract_id: str
    target_layer: str
    version: str
    checksum: str
    fields: list[str]
    field_types: dict[str, str]
    primary_keys: list[str]
    business_keys: list[str]
    hash_keys: list[str]

    @property
    def key_fields(self) -> list[str]:
        if self.primary_keys:
            return self.primary_keys
        if self.business_keys:
            return self.business_keys
        return []

    @property
    def effective_hash_fields(self) -> list[str]:
        if self.hash_keys:
            return self.hash_keys
        return self.fields

    def validate(self) -> None:
        if not self.contract_id:
            raise ValueError("contract.id is required")
        if not self.fields:
            raise ValueError("version.schema_json.fields must not be empty")
        if not self.key_fields:
            raise ValueError("Contract keys are empty: provide keys.primary or keys.business")

        missing_keys = [key for key in self.key_fields if key not in self.fields]
        if missing_keys:
            raise ValueError(f"Key fields are absent in fields: {missing_keys}")

        missing_hash_fields = [key for key in self.effective_hash_fields if key not in self.fields]
        if missing_hash_fields:
            raise ValueError(f"Hash fields are absent in fields: {missing_hash_fields}")

        unknown_typed_fields = [field for field in self.field_types if field not in self.fields]
        if unknown_typed_fields:
            raise ValueError(f"Typed fields are absent in fields: {unknown_typed_fields}")

    @classmethod
    def from_registry_payload(cls, payload: Mapping[str, Any]) -> "ContractDefinition":
        contract = cls(
            contract_id=str(payload.get("contract_id", "")),
            target_layer=str(payload.get("target_layer", "")),
            version=str(payload.get("version", "")),
            checksum=str(payload.get("checksum", "")),
            fields=[str(v) for v in (payload.get("fields") or [])],
            field_types={str(k): str(v) for k, v in dict(payload.get("field_types") or {}).items()},
            primary_keys=[str(v) for v in (payload.get("primary_keys") or [])],
            business_keys=[str(v) for v in (payload.get("business_keys") or [])],
            hash_keys=[str(v) for v in (payload.get("hash_keys") or [])],
        )
        contract.validate()
        return contract


@dataclass(frozen=True)
class HashDiffResult:
    read_count: int
    insert_count: int
    update_count: int
    delete_count: int
    unchanged_count: int
    processed_batches: int
    source_read_seconds: float
    diff_seconds: float
    write_seconds: float
    total_seconds: float

    def metrics_dict(self) -> dict[str, float | int]:
        return {
            "processed_batches": self.processed_batches,
            "source_read_seconds": self.source_read_seconds,
            "diff_seconds": self.diff_seconds,
            "write_seconds": self.write_seconds,
            "total_seconds": self.total_seconds,
            "delete_count": self.delete_count,
        }


def build_row_key(row: Mapping[str, Any], key_fields: Sequence[str]) -> tuple[Any, ...]:
    return tuple(row[field] for field in key_fields)


def classify_changes(
    source_rows: Sequence[Mapping[str, Any]],
    existing_hashes: Mapping[tuple[Any, ...], str],
    key_fields: Sequence[str],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], int]:
    inserts: list[dict[str, Any]] = []
    updates: list[dict[str, Any]] = []
    unchanged_count = 0

    for row in source_rows:
        row_dict = dict(row)
        key = build_row_key(row_dict, key_fields)

        current_hash = row_dict["row_hash"]
        previous_hash = existing_hashes.get(key)

        if previous_hash is None:
            inserts.append(row_dict)
            continue
        if previous_hash != current_hash:
            updates.append(row_dict)
            continue
        unchanged_count += 1

    return inserts, updates, unchanged_count


def _normalize_contract_type(contract_type: str) -> str:
    normalized = contract_type.strip().lower()
    if normalized.startswith("array"):
        return "array"
    if normalized in {"uuid"}:
        return "uuid"
    if normalized in {"bool", "boolean"}:
        return "boolean"
    if normalized in {"date"}:
        return "date"
    if normalized in {"time"}:
        return "time"
    if normalized in {
        "datetime",
        "timestamp",
        "timestampz",
        "timestamptz",
        "timestamp without time zone",
        "timestamp with time zone",
    }:
        return "timestamp"
    if normalized in {
        "decimal",
        "numeric",
        "number",
        "double",
        "double precision",
        "float",
        "float4",
        "float8",
        "real",
        "money",
    }:
        return "decimal"
    if normalized in {
        "tinyint",
        "smallint",
        "int",
        "integer",
        "bigint",
        "serial",
        "bigserial",
    }:
        return "integer"
    if normalized in {
        "string",
        "text",
        "char",
        "character",
        "varchar",
        "character varying",
        "citext",
    }:
        return "string"
    if normalized in {"json", "jsonb"}:
        return "json"
    if normalized in {"binary", "bytea", "blob"}:
        return "binary"
    return "unknown"


def _normalize_sqlalchemy_type(column_type: Any) -> str:
    if isinstance(column_type, Boolean):
        return "boolean"
    if isinstance(column_type, DateTime):
        return "timestamp"
    if isinstance(column_type, Date):
        return "date"
    if isinstance(column_type, Time):
        return "time"
    if isinstance(column_type, Numeric):
        return "decimal"
    if isinstance(column_type, Integer):
        return "integer"
    if isinstance(column_type, String):
        return "string"
    if isinstance(column_type, JSON):
        return "json"
    if isinstance(column_type, LargeBinary):
        return "binary"

    type_name = str(column_type).lower()
    if "uuid" in type_name:
        return "uuid"
    if "timestamp" in type_name:
        return "timestamp"
    if type_name.startswith("date"):
        return "date"
    if type_name.startswith("time"):
        return "time"
    if "bool" in type_name:
        return "boolean"
    if any(token in type_name for token in ("numeric", "decimal", "float", "double", "real", "money")):
        return "decimal"
    if any(token in type_name for token in ("int", "serial")):
        return "integer"
    if any(token in type_name for token in ("char", "text", "string", "citext")):
        return "string"
    if "json" in type_name:
        return "json"
    if any(token in type_name for token in ("bytea", "blob", "binary")):
        return "binary"
    if "array" in type_name:
        return "array"
    return "unknown"


def _validate_source_columns(
    source_table: Table,
    fields: Sequence[str],
    field_types: Mapping[str, str] | None = None,
) -> None:
    available = set(source_table.columns.keys())
    missing = [field for field in fields if field not in available]
    if missing:
        raise ValueError(f"Source table is missing required contract fields: {missing}")

    if not field_types:
        return

    mismatches: list[str] = []
    for field in fields:
        expected_raw = field_types.get(field)
        if not expected_raw:
            continue

        expected_type = _normalize_contract_type(expected_raw)
        if expected_type == "unknown":
            mismatches.append(
                f'{field}: unsupported contract type "{expected_raw}"'
            )
            continue

        source_column = source_table.c[field]
        actual_type = _normalize_sqlalchemy_type(source_column.type)
        if actual_type != expected_type:
            mismatches.append(
                f"{field}: expected {expected_type} ({expected_raw}), "
                f"got {actual_type} ({source_column.type})"
            )

    if mismatches:
        raise ValueError(
            "Source column type mismatch against contract: "
            + "; ".join(mismatches)
        )


def _make_index_name(table_name: str, suffix: str) -> str:
    return f"idx_{table_name}_{suffix}"[:63]


def _ensure_target_indexes(target_engine, target_schema: str, target_table_name: str) -> None:
    row_hash_index_name = _make_index_name(target_table_name, "row_hash")

    with target_engine.begin() as conn:
        conn.execute(
            text(
                f'CREATE INDEX IF NOT EXISTS "{row_hash_index_name}" '
                f'ON "{target_schema}"."{target_table_name}" ("row_hash")'
            )
        )


def _ensure_target_curated_table(
    target_engine,
    target_schema: str,
    target_table_name: str,
    source_table: Table,
    fields: Sequence[str],
    key_fields: Sequence[str],
) -> Table:
    ensure_schema(target_engine, target_schema)

    if not table_exists(target_engine, target_schema, target_table_name):
        metadata = MetaData()
        columns = []

        for field in fields:
            source_column = source_table.c[field]
            nullable = source_column.nullable and field not in key_fields
            columns.append(Column(field, source_column.type, nullable=nullable))

        columns.append(Column("row_hash", Text, nullable=False))

        table = Table(
            target_table_name,
            metadata,
            *columns,
            PrimaryKeyConstraint(*key_fields, name=f"pk_{target_table_name}"),
            schema=target_schema,
        )

        metadata.create_all(target_engine, tables=[table], checkfirst=True)

    target_table = reflect_table(target_engine, target_schema, target_table_name)
    missing_columns = (set(fields) | {"row_hash"}) - set(target_table.columns.keys())
    if missing_columns:
        raise ValueError(f"Target table is missing required columns: {sorted(missing_columns)}")

    _ensure_target_indexes(target_engine, target_schema, target_table_name)
    return target_table


def _make_key_staging_table_name(target_table_name: str) -> str:
    suffix = uuid.uuid4().hex[:10]
    return f"_hd_keys_{target_table_name}_{suffix}"[:63]


def _create_key_staging_table(
    target_engine,
    target_schema: str,
    target_table: Table,
    key_fields: Sequence[str],
) -> Table:
    metadata = MetaData()
    staging_table = Table(
        _make_key_staging_table_name(target_table.name),
        metadata,
        *(Column(field, target_table.c[field].type, nullable=True) for field in key_fields),
        schema=target_schema,
    )
    metadata.create_all(target_engine, tables=[staging_table], checkfirst=False)
    return staging_table


def _stage_source_keys(
    target_engine,
    staging_table: Table,
    key_fields: Sequence[str],
    key_tuples: Sequence[tuple[Any, ...]],
    batch_size: int,
) -> None:
    if not key_tuples:
        return

    unique_keys = list(dict.fromkeys(key_tuples))
    key_rows = [
        {field: key_tuple[index] for index, field in enumerate(key_fields)}
        for key_tuple in unique_keys
    ]

    with target_engine.begin() as conn:
        for key_rows_chunk in _chunk_rows(key_rows, batch_size):
            conn.execute(staging_table.insert(), key_rows_chunk)


def _delete_missing_target_rows(
    target_engine,
    target_table: Table,
    staging_table: Table,
    key_fields: Sequence[str],
) -> int:
    key_match_predicate = and_(
        *(
            target_table.c[field].is_not_distinct_from(staging_table.c[field])
            for field in key_fields
        )
    )
    source_key_exists = exists(
        select(1).select_from(staging_table).where(key_match_predicate)
    )
    delete_stmt = target_table.delete().where(~source_key_exists)

    with target_engine.begin() as conn:
        result = conn.execute(delete_stmt)

    return int(result.rowcount or 0)


def _drop_staging_table(target_engine, staging_table: Table) -> None:
    staging_table.drop(target_engine, checkfirst=True)


def _iter_source_row_batches(
    source_engine,
    source_table: Table,
    fields: Sequence[str],
    key_fields: Sequence[str],
    source_batch_size: int,
) -> Iterable[tuple[list[dict[str, Any]], float]]:
    query = select(*(source_table.c[field] for field in fields))
    if key_fields:
        query = query.order_by(*(source_table.c[field] for field in key_fields))

    with source_engine.connect() as conn:
        result = conn.execution_options(stream_results=True).execute(query).mappings()

        while True:
            fetch_started = perf_counter()
            rows = result.fetchmany(source_batch_size)
            fetch_seconds = perf_counter() - fetch_started
            if not rows:
                break

            normalized: list[dict[str, Any]] = []
            for row in rows:
                normalized.append({field: row[field] for field in fields})
            yield normalized, fetch_seconds


def _add_row_hashes(rows: Sequence[Mapping[str, Any]], hash_fields: Sequence[str]) -> list[dict[str, Any]]:
    result: list[dict[str, Any]] = []
    for row in rows:
        row_dict = dict(row)
        row_dict["row_hash"] = calculate_row_hash(row_dict, hash_fields)
        result.append(row_dict)
    return result


def _read_existing_hashes_for_keys(
    target_engine,
    target_table: Table,
    key_fields: Sequence[str],
    key_tuples: Sequence[tuple[Any, ...]],
) -> dict[tuple[Any, ...], str]:
    if not key_tuples:
        return {}

    unique_keys = list(dict.fromkeys(key_tuples))

    with target_engine.connect() as conn:
        if len(key_fields) == 1:
            key_field = key_fields[0]
            key_values = [key[0] for key in unique_keys]
            query = select(target_table.c[key_field], target_table.c.row_hash).where(
                target_table.c[key_field].in_(key_values)
            )
        else:
            key_columns = [target_table.c[field] for field in key_fields]
            query = select(*key_columns, target_table.c.row_hash).where(
                tuple_(*key_columns).in_(unique_keys)
            )

        rows = conn.execute(query).mappings().all()

    return {build_row_key(row, key_fields): row["row_hash"] for row in rows}


def _chunk_rows(rows: Sequence[Mapping[str, Any]], chunk_size: int) -> Iterable[list[Mapping[str, Any]]]:
    for index in range(0, len(rows), chunk_size):
        yield list(rows[index : index + chunk_size])


def _upsert_rows(
    target_engine,
    target_table: Table,
    rows: Sequence[Mapping[str, Any]],
    key_fields: Sequence[str],
    fields: Sequence[str],
    upsert_batch_size: int,
) -> None:
    if not rows:
        return

    update_fields = [field for field in fields if field not in key_fields] + ["row_hash"]

    with target_engine.begin() as conn:
        # Keep statement deterministic for testing/debugging
        sorted_rows = sorted(rows, key=lambda row: build_row_key(row, key_fields))
        for rows_chunk in _chunk_rows(sorted_rows, upsert_batch_size):
            insert_stmt = pg_insert(target_table).values(rows_chunk)
            upsert_stmt = insert_stmt.on_conflict_do_update(
                index_elements=list(key_fields),
                set_={field: insert_stmt.excluded[field] for field in update_fields},
            )
            conn.execute(upsert_stmt)


def run_hash_diff(
    source_dsn: str,
    source_table: str,
    target_dsn: str,
    target_table_curated: str,
    contract: ContractDefinition,
    source_batch_size: int = 1000,
    upsert_batch_size: int = 1000,
) -> HashDiffResult:
    if source_batch_size <= 0:
        raise ValueError("source_batch_size must be greater than zero")
    if upsert_batch_size <= 0:
        raise ValueError("upsert_batch_size must be greater than zero")

    source_engine = create_sqlalchemy_engine(source_dsn)
    target_engine = create_sqlalchemy_engine(target_dsn)

    started_at = perf_counter()
    read_seconds = 0.0
    diff_seconds = 0.0
    write_seconds = 0.0

    read_count = 0
    insert_count = 0
    update_count = 0
    delete_count = 0
    unchanged_count = 0
    processed_batches = 0
    staged_keys_table: Table | None = None

    try:
        source_schema, source_name = parse_table_name(source_table)
        target_schema, target_name = parse_table_name(target_table_curated)

        source_table_obj = reflect_table(source_engine, source_schema, source_name)
        _validate_source_columns(
            source_table=source_table_obj,
            fields=contract.fields,
            field_types=contract.field_types,
        )

        target_table_obj = _ensure_target_curated_table(
            target_engine=target_engine,
            target_schema=target_schema,
            target_table_name=target_name,
            source_table=source_table_obj,
            fields=contract.fields,
            key_fields=contract.key_fields,
        )

        staged_keys_table = _create_key_staging_table(
            target_engine=target_engine,
            target_schema=target_schema,
            target_table=target_table_obj,
            key_fields=contract.key_fields,
        )

        try:
            for source_batch, fetch_seconds in _iter_source_row_batches(
                source_engine=source_engine,
                source_table=source_table_obj,
                fields=contract.fields,
                key_fields=contract.key_fields,
                source_batch_size=source_batch_size,
            ):
                processed_batches += 1
                read_seconds += fetch_seconds
                read_count += len(source_batch)

                diff_started = perf_counter()
                source_rows_with_hashes = _add_row_hashes(source_batch, contract.effective_hash_fields)
                batch_keys = [build_row_key(row, contract.key_fields) for row in source_rows_with_hashes]
                _stage_source_keys(
                    target_engine=target_engine,
                    staging_table=staged_keys_table,
                    key_fields=contract.key_fields,
                    key_tuples=batch_keys,
                    batch_size=upsert_batch_size,
                )
                existing_hashes = _read_existing_hashes_for_keys(
                    target_engine=target_engine,
                    target_table=target_table_obj,
                    key_fields=contract.key_fields,
                    key_tuples=batch_keys,
                )
                inserts, updates, unchanged_batch = classify_changes(
                    source_rows=source_rows_with_hashes,
                    existing_hashes=existing_hashes,
                    key_fields=contract.key_fields,
                )
                diff_seconds += perf_counter() - diff_started

                unchanged_count += unchanged_batch
                insert_count += len(inserts)
                update_count += len(updates)

                write_started = perf_counter()
                _upsert_rows(
                    target_engine=target_engine,
                    target_table=target_table_obj,
                    rows=[*inserts, *updates],
                    key_fields=contract.key_fields,
                    fields=contract.fields,
                    upsert_batch_size=upsert_batch_size,
                )
                write_seconds += perf_counter() - write_started

            delete_started = perf_counter()
            delete_count = _delete_missing_target_rows(
                target_engine=target_engine,
                target_table=target_table_obj,
                staging_table=staged_keys_table,
                key_fields=contract.key_fields,
            )
            write_seconds += perf_counter() - delete_started
        finally:
            _drop_staging_table(target_engine=target_engine, staging_table=staged_keys_table)

        total_seconds = perf_counter() - started_at
        return HashDiffResult(
            read_count=read_count,
            insert_count=insert_count,
            update_count=update_count,
            delete_count=delete_count,
            unchanged_count=unchanged_count,
            processed_batches=processed_batches,
            source_read_seconds=read_seconds,
            diff_seconds=diff_seconds,
            write_seconds=write_seconds,
            total_seconds=total_seconds,
        )
    finally:
        source_engine.dispose()
        target_engine.dispose()
