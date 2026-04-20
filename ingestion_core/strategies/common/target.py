from __future__ import annotations

from typing import Any, Mapping, Sequence

from sqlalchemy import Column, MetaData, PrimaryKeyConstraint, Table, Text, tuple_
from sqlalchemy.dialects.postgresql import insert as pg_insert

from ingestion_core.adapters.postgres import ensure_schema, reflect_table, table_exists
from ingestion_core.contracts.runtime import contract_field_nullable, sqlalchemy_type_from_contract_field
from ingestion_core.contracts.types import ContractDefinition
from ingestion_core.strategies.common.change_detection import build_row_key, chunk_rows


def make_hash_state_table_name(target_table_name: str) -> str:
    return f"_hd_state_{target_table_name}"[:63]


def ensure_target_table_from_contract(
    target_engine,
    target_schema: str,
    target_table_name: str,
    contract: ContractDefinition,
) -> Table:
    ensure_schema(target_engine, target_schema)

    if not table_exists(target_engine, target_schema, target_table_name):
        metadata = MetaData()
        table = Table(
            target_table_name,
            metadata,
            *(
                Column(
                    field,
                    sqlalchemy_type_from_contract_field(contract, field),
                    nullable=contract_field_nullable(contract, field),
                )
                for field in contract.fields
            ),
            PrimaryKeyConstraint(*contract.key_fields, name=f"pk_{target_table_name}"[:63]),
            schema=target_schema,
        )
        metadata.create_all(target_engine, tables=[table], checkfirst=True)

    target_table = reflect_table(target_engine, target_schema, target_table_name)
    missing_columns = set(contract.fields) - set(target_table.columns.keys())
    if missing_columns:
        raise ValueError(f"Target table is missing required columns: {sorted(missing_columns)}")

    return target_table


def ensure_hash_state_table(
    target_engine,
    target_schema: str,
    target_table_name: str,
    target_table: Table,
    key_fields: Sequence[str],
) -> Table:
    hash_state_table_name = make_hash_state_table_name(target_table_name)

    if not table_exists(target_engine, target_schema, hash_state_table_name):
        metadata = MetaData()
        columns = [
            Column(field, target_table.c[field].type, nullable=target_table.c[field].nullable)
            for field in key_fields
        ]
        columns.append(Column("row_hash", Text, nullable=False))

        table = Table(
            hash_state_table_name,
            metadata,
            *columns,
            PrimaryKeyConstraint(*key_fields, name=f"pk_{hash_state_table_name}"[:63]),
            schema=target_schema,
        )
        metadata.create_all(target_engine, tables=[table], checkfirst=True)

    hash_state_table = reflect_table(target_engine, target_schema, hash_state_table_name)
    missing_columns = set(key_fields) | {"row_hash"}
    actual_columns = set(hash_state_table.columns.keys())
    if not missing_columns.issubset(actual_columns):
        raise ValueError(
            f"Hash state table is missing required columns: {sorted(missing_columns - actual_columns)}"
        )

    return hash_state_table


def upsert_changed_rows(
    target_engine,
    target_table: Table,
    hash_state_table: Table,
    rows: Sequence[Mapping[str, Any]],
    key_fields: Sequence[str],
    fields: Sequence[str],
    upsert_batch_size: int,
) -> None:
    if not rows:
        return

    target_update_fields = [field for field in fields if field not in key_fields]

    with target_engine.begin() as conn:
        sorted_rows = sorted(rows, key=lambda row: build_row_key(row, key_fields))
        for rows_chunk in chunk_rows(sorted_rows, upsert_batch_size):
            target_rows_chunk = [{field: row[field] for field in fields} for row in rows_chunk]
            target_insert_stmt = pg_insert(target_table).values(target_rows_chunk)
            if target_update_fields:
                target_upsert_stmt = target_insert_stmt.on_conflict_do_update(
                    index_elements=list(key_fields),
                    set_={field: target_insert_stmt.excluded[field] for field in target_update_fields},
                )
            else:
                target_upsert_stmt = target_insert_stmt.on_conflict_do_nothing(
                    index_elements=list(key_fields),
                )
            conn.execute(target_upsert_stmt)

            hash_rows_chunk = [
                {
                    **{field: row[field] for field in key_fields},
                    "row_hash": row["row_hash"],
                }
                for row in rows_chunk
            ]
            hash_insert_stmt = pg_insert(hash_state_table).values(hash_rows_chunk)
            hash_upsert_stmt = hash_insert_stmt.on_conflict_do_update(
                index_elements=list(key_fields),
                set_={"row_hash": hash_insert_stmt.excluded.row_hash},
            )
            conn.execute(hash_upsert_stmt)


def delete_rows_by_keys(
    target_engine,
    target_table: Table,
    hash_state_table: Table,
    key_fields: Sequence[str],
    key_tuples: Sequence[tuple[Any, ...]],
    batch_size: int,
) -> int:
    if not key_tuples:
        return 0

    unique_keys = list(dict.fromkeys(key_tuples))
    target_delete_count = 0

    with target_engine.begin() as conn:
        for keys_chunk in chunk_rows(unique_keys, batch_size):
            for table in (target_table, hash_state_table):
                if len(key_fields) == 1:
                    key_field = key_fields[0]
                    values = [key[0] for key in keys_chunk]
                    delete_stmt = table.delete().where(table.c[key_field].in_(values))
                else:
                    key_columns = [table.c[field] for field in key_fields]
                    delete_stmt = table.delete().where(tuple_(*key_columns).in_(keys_chunk))
                result = conn.execute(delete_stmt)
                if table is target_table:
                    target_delete_count += int(result.rowcount or 0)

    return target_delete_count
