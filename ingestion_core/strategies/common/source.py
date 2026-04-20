from __future__ import annotations

from typing import Any, Mapping, Sequence

from sqlalchemy import Table
from sqlalchemy.sql.sqltypes import Boolean, Date, DateTime, Integer, JSON, LargeBinary, Numeric, String, Time


def normalize_contract_type(contract_type: str) -> str:
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


def normalize_sqlalchemy_type(column_type: Any) -> str:
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


def validate_source_columns(
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

        expected_type = normalize_contract_type(expected_raw)
        if expected_type == "unknown":
            mismatches.append(f'{field}: unsupported contract type "{expected_raw}"')
            continue

        source_column = source_table.c[field]
        actual_type = normalize_sqlalchemy_type(source_column.type)
        if expected_type == "array" and actual_type == "json":
            continue
        if actual_type != expected_type:
            mismatches.append(
                f"{field}: expected {expected_type} ({expected_raw}), "
                f"got {actual_type} ({source_column.type})"
            )

    if mismatches:
        raise ValueError(
            "Source column type mismatch against contract: " + "; ".join(mismatches)
        )
