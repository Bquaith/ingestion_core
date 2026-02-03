from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping, Sequence

from sqlalchemy import Column, MetaData, PrimaryKeyConstraint, Table, Text, select
from sqlalchemy.dialects.postgresql import insert as pg_insert

from ingestion_platform.hashing import calculate_row_hash
from ingestion_platform.postgres import (
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

    @classmethod
    def from_registry_payload(cls, payload: Mapping[str, Any]) -> "ContractDefinition":
        contract = cls(
            contract_id=str(payload.get("contract_id", "")),
            target_layer=str(payload.get("target_layer", "")),
            version=str(payload.get("version", "")),
            checksum=str(payload.get("checksum", "")),
            fields=[str(v) for v in (payload.get("fields") or [])],
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
    unchanged_count: int


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


def _validate_source_columns(source_table: Table, fields: Sequence[str]) -> None:
    available = set(source_table.columns.keys())
    missing = [field for field in fields if field not in available]
    if missing:
        raise ValueError(f"Source table is missing required contract fields: {missing}")


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

    return target_table


def _read_source_rows(source_engine, source_table: Table, fields: Sequence[str]) -> list[dict[str, Any]]:
    query = select(*(source_table.c[field] for field in fields))

    with source_engine.connect() as conn:
        rows = conn.execute(query).mappings().all()

    normalized: list[dict[str, Any]] = []
    for row in rows:
        normalized.append({field: row[field] for field in fields})

    return normalized


def _add_row_hashes(rows: Sequence[Mapping[str, Any]], hash_fields: Sequence[str]) -> list[dict[str, Any]]:
    result: list[dict[str, Any]] = []
    for row in rows:
        row_dict = dict(row)
        row_dict["row_hash"] = calculate_row_hash(row_dict, hash_fields)
        result.append(row_dict)
    return result


def _read_existing_hashes(
    target_engine,
    target_table: Table,
    key_fields: Sequence[str],
) -> dict[tuple[Any, ...], str]:
    query = select(*(target_table.c[field] for field in key_fields), target_table.c.row_hash)

    with target_engine.connect() as conn:
        rows = conn.execute(query).mappings().all()

    return {build_row_key(row, key_fields): row["row_hash"] for row in rows}


def _upsert_rows(
    target_engine,
    target_table: Table,
    rows: Sequence[Mapping[str, Any]],
    key_fields: Sequence[str],
    fields: Sequence[str],
) -> None:
    if not rows:
        return

    # Keep statement deterministic for testing/debugging
    sorted_rows = sorted(rows, key=lambda row: build_row_key(row, key_fields))

    insert_stmt = pg_insert(target_table).values(sorted_rows)
    update_fields = [field for field in fields if field not in key_fields] + ["row_hash"]

    upsert_stmt = insert_stmt.on_conflict_do_update(
        index_elements=list(key_fields),
        set_={field: insert_stmt.excluded[field] for field in update_fields},
    )

    with target_engine.begin() as conn:
        conn.execute(upsert_stmt)


def run_hash_diff(
    source_dsn: str,
    source_table: str,
    target_dsn: str,
    target_table_curated: str,
    contract: ContractDefinition,
) -> HashDiffResult:
    source_engine = create_sqlalchemy_engine(source_dsn)
    target_engine = create_sqlalchemy_engine(target_dsn)

    try:
        source_schema, source_name = parse_table_name(source_table)
        target_schema, target_name = parse_table_name(target_table_curated)

        source_table_obj = reflect_table(source_engine, source_schema, source_name)
        _validate_source_columns(source_table_obj, contract.fields)

        target_table_obj = _ensure_target_curated_table(
            target_engine=target_engine,
            target_schema=target_schema,
            target_table_name=target_name,
            source_table=source_table_obj,
            fields=contract.fields,
            key_fields=contract.key_fields,
        )

        source_rows = _read_source_rows(source_engine, source_table_obj, contract.fields)
        source_rows_with_hashes = _add_row_hashes(source_rows, contract.effective_hash_fields)

        existing_hashes = _read_existing_hashes(target_engine, target_table_obj, contract.key_fields)
        inserts, updates, unchanged_count = classify_changes(
            source_rows=source_rows_with_hashes,
            existing_hashes=existing_hashes,
            key_fields=contract.key_fields,
        )

        _upsert_rows(
            target_engine=target_engine,
            target_table=target_table_obj,
            rows=[*inserts, *updates],
            key_fields=contract.key_fields,
            fields=contract.fields,
        )

        return HashDiffResult(
            read_count=len(source_rows_with_hashes),
            insert_count=len(inserts),
            update_count=len(updates),
            unchanged_count=unchanged_count,
        )
    finally:
        source_engine.dispose()
        target_engine.dispose()
