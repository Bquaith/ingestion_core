from __future__ import annotations

from typing import Any, Iterable, Mapping, Sequence

from sqlalchemy import Table, select, tuple_


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


def chunk_rows(rows: Sequence[Mapping[str, Any]], chunk_size: int) -> Iterable[list[Mapping[str, Any]]]:
    for index in range(0, len(rows), chunk_size):
        yield list(rows[index : index + chunk_size])


def read_existing_hashes_for_keys(
    target_engine,
    hash_state_table: Table,
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
            query = select(hash_state_table.c[key_field], hash_state_table.c.row_hash).where(
                hash_state_table.c[key_field].in_(key_values)
            )
        else:
            key_columns = [hash_state_table.c[field] for field in key_fields]
            query = select(*key_columns, hash_state_table.c.row_hash).where(
                tuple_(*key_columns).in_(unique_keys)
            )

        rows = conn.execute(query).mappings().all()

    return {build_row_key(row, key_fields): row["row_hash"] for row in rows}
