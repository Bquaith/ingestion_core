from __future__ import annotations

import uuid
from dataclasses import dataclass
from time import perf_counter
from typing import Any, Iterable, Mapping, Sequence

from sqlalchemy import Column, MetaData, PrimaryKeyConstraint, Table, Text, and_, exists, select, tuple_

from ingestion_core.adapters.postgres import (
    create_sqlalchemy_engine,
    ensure_schema,
    parse_table_name,
    reflect_table,
    table_exists,
)
from ingestion_core.contracts.types import ContractDefinition
from ingestion_core.strategies.common.change_detection import (
    build_row_key,
    chunk_rows as _chunk_rows,
    classify_changes,
    read_existing_hashes_for_keys as _read_existing_hashes_for_keys,
)
from ingestion_core.strategies.common.source import (
    normalize_contract_type as _normalize_contract_type,
    normalize_sqlalchemy_type as _normalize_sqlalchemy_type,
    validate_source_columns as _validate_source_columns,
)
from ingestion_core.strategies.common.target import (
    ensure_hash_state_table as _ensure_hash_state_table,
    make_hash_state_table_name as _make_hash_state_table_name,
    upsert_changed_rows as _upsert_changed_rows,
)
from ingestion_core.utils.hashing import calculate_row_hash

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

        table = Table(
            target_table_name,
            metadata,
            *columns,
            PrimaryKeyConstraint(*key_fields, name=f"pk_{target_table_name}"),
            schema=target_schema,
        )

        metadata.create_all(target_engine, tables=[table], checkfirst=True)

    target_table = reflect_table(target_engine, target_schema, target_table_name)
    missing_columns = set(fields) - set(target_table.columns.keys())
    if missing_columns:
        raise ValueError(f"Target table is missing required columns: {sorted(missing_columns)}")

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


def _drop_staging_table(target_engine, staging_table: Table) -> None:
    staging_table.drop(target_engine, checkfirst=True)


def _iter_source_row_batches(
    source_engine,
    source_table: Table,
    fields: Sequence[str],
    key_fields: Sequence[str],
    source_batch_size: int,
    extra_fields: Sequence[str] | None = None,
) -> Iterable[tuple[list[dict[str, Any]], float]]:
    selected_fields = list(fields)
    for extra_field in extra_fields or ():
        if extra_field not in selected_fields:
            selected_fields.append(extra_field)

    query = select(*(source_table.c[field] for field in selected_fields))
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
                normalized.append({field: row[field] for field in selected_fields})
            yield normalized, fetch_seconds


def _add_row_hashes(rows: Sequence[Mapping[str, Any]], hash_fields: Sequence[str]) -> list[dict[str, Any]]:
    result: list[dict[str, Any]] = []
    for row in rows:
        row_dict = dict(row)
        row_dict["row_hash"] = calculate_row_hash(row_dict, hash_fields)
        result.append(row_dict)
    return result


def _use_precomputed_row_hashes(
    rows: Sequence[Mapping[str, Any]],
    fields: Sequence[str],
) -> list[dict[str, Any]]:
    result: list[dict[str, Any]] = []
    for row in rows:
        row_hash = row.get("row_hash")
        if row_hash is None or not str(row_hash).strip():
            raise ValueError("Source row_hash column must contain non-empty values for all rows")

        row_dict = {field: row[field] for field in fields}
        row_dict["row_hash"] = str(row_hash)
        result.append(row_dict)
    return result

def _delete_missing_rows(
    target_engine,
    target_table: Table,
    hash_state_table: Table,
    staging_table: Table,
    key_fields: Sequence[str],
) -> int:
    target_delete_count = 0

    with target_engine.begin() as conn:
        for table in (target_table, hash_state_table):
            key_match_predicate = and_(
                *(
                    table.c[field].is_not_distinct_from(staging_table.c[field])
                    for field in key_fields
                )
            )
            source_key_exists = exists(
                select(1).select_from(staging_table).where(key_match_predicate)
            )
            delete_stmt = table.delete().where(~source_key_exists)
            result = conn.execute(delete_stmt)
            if table is target_table:
                target_delete_count = int(result.rowcount or 0)

    return target_delete_count


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
    hash_state_table: Table | None = None

    try:
        source_schema, source_name = parse_table_name(source_table)
        target_schema, target_name = parse_table_name(target_table_curated)

        source_table_obj = reflect_table(source_engine, source_schema, source_name)
        _validate_source_columns(
            source_table=source_table_obj,
            fields=contract.fields,
            field_types=contract.field_types,
        )
        has_precomputed_row_hash = "row_hash" in source_table_obj.columns.keys()

        target_table_obj = _ensure_target_curated_table(
            target_engine=target_engine,
            target_schema=target_schema,
            target_table_name=target_name,
            source_table=source_table_obj,
            fields=contract.fields,
            key_fields=contract.key_fields,
        )
        hash_state_table = _ensure_hash_state_table(
            target_engine=target_engine,
            target_schema=target_schema,
            target_table_name=target_name,
            target_table=target_table_obj,
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
                extra_fields=["row_hash"] if has_precomputed_row_hash else None,
            ):
                processed_batches += 1
                read_seconds += fetch_seconds
                read_count += len(source_batch)

                diff_started = perf_counter()
                if has_precomputed_row_hash:
                    source_rows_with_hashes = _use_precomputed_row_hashes(
                        source_batch,
                        contract.fields,
                    )
                else:
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
                    hash_state_table=hash_state_table,
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
                _upsert_changed_rows(
                    target_engine=target_engine,
                    target_table=target_table_obj,
                    hash_state_table=hash_state_table,
                    rows=[*inserts, *updates],
                    key_fields=contract.key_fields,
                    fields=contract.fields,
                    upsert_batch_size=upsert_batch_size,
                )
                write_seconds += perf_counter() - write_started

            delete_started = perf_counter()
            delete_count = _delete_missing_rows(
                target_engine=target_engine,
                target_table=target_table_obj,
                hash_state_table=hash_state_table,
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
