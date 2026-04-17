from __future__ import annotations

import base64
from dataclasses import dataclass
from datetime import date, datetime, time
from decimal import Decimal, InvalidOperation
from typing import Any, Mapping

from sqlalchemy import Boolean, Date, DateTime, Integer, LargeBinary, Numeric, Text, Time
from sqlalchemy.dialects.postgresql import JSONB, UUID as PGUUID

from ingestion_core.contracts.schema_validation import validate_instance_against_schema
from ingestion_core.contracts.types import ContractDefinition
from ingestion_core.utils.hashing import calculate_row_hash


class ContractValidationError(RuntimeError):
    """Raised when extracted rows do not satisfy the active contract."""


@dataclass(frozen=True)
class ContractRowValidationResult:
    normalized_row: dict[str, Any]
    errors: list[dict[str, Any]]


def json_default(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, time):
        return value.isoformat()
    if isinstance(value, Decimal):
        return format(value, "f")
    if isinstance(value, (bytes, bytearray, memoryview)):
        return base64.b64encode(bytes(value)).decode("ascii")
    raise TypeError(f"Unsupported value for JSON serialization: {type(value)!r}")


def normalize_json_value(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(key): normalize_json_value(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [normalize_json_value(item) for item in value]
    if isinstance(value, (datetime, date, time, Decimal, bytes, bytearray, memoryview)):
        return json_default(value)
    return value


def parse_iso_datetime(value: str) -> datetime:
    trimmed = value.strip()
    if trimmed.endswith("Z"):
        trimmed = f"{trimmed[:-1]}+00:00"
    return datetime.fromisoformat(trimmed)


def coerce_contract_value(value: Any, field_type: str | None) -> Any:
    if value is None:
        return None

    normalized_type = (field_type or "").strip().lower()
    if normalized_type in {"", "unknown"}:
        return value
    if normalized_type.startswith("array"):
        if isinstance(value, list):
            return value
        if isinstance(value, tuple):
            return list(value)
        raise ValueError("expected array value")
    if normalized_type in {"string", "text", "char", "varchar", "character varying"}:
        return str(value)
    if normalized_type == "uuid":
        return str(value)
    if normalized_type in {"integer", "int", "bigint", "smallint", "serial", "bigserial"}:
        if isinstance(value, bool):
            raise ValueError("boolean is not a valid integer")
        return int(value)
    if normalized_type in {
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
        try:
            return Decimal(str(value))
        except (InvalidOperation, ValueError) as exc:
            raise ValueError("expected decimal value") from exc
    if normalized_type in {"bool", "boolean"}:
        if isinstance(value, bool):
            return value
        if isinstance(value, str):
            lowered = value.strip().lower()
            if lowered in {"true", "1", "yes", "y"}:
                return True
            if lowered in {"false", "0", "no", "n"}:
                return False
        raise ValueError("expected boolean value")
    if normalized_type in {
        "datetime",
        "timestamp",
        "timestampz",
        "timestamptz",
        "timestamp without time zone",
        "timestamp with time zone",
    }:
        if isinstance(value, datetime):
            return value
        if isinstance(value, str):
            return parse_iso_datetime(value)
        raise ValueError("expected timestamp value")
    if normalized_type == "date":
        if isinstance(value, date) and not isinstance(value, datetime):
            return value
        if isinstance(value, str):
            return date.fromisoformat(value)
        raise ValueError("expected date value")
    if normalized_type == "time":
        if isinstance(value, time):
            return value
        if isinstance(value, str):
            return time.fromisoformat(value)
        raise ValueError("expected time value")
    if normalized_type in {"json", "jsonb"}:
        if isinstance(value, (dict, list)):
            return value
        raise ValueError("expected JSON object or array")
    if normalized_type in {"binary", "bytea", "blob"}:
        if isinstance(value, (bytes, bytearray, memoryview)):
            return bytes(value)
        if isinstance(value, str):
            try:
                return base64.b64decode(value.encode("ascii"))
            except ValueError as exc:
                raise ValueError("expected base64-encoded binary value") from exc
        raise ValueError("expected binary value")
    return value


def summarize_validation_errors(errors: list[dict[str, Any]]) -> str:
    if not errors:
        return "no validation error details captured"

    chunks: list[str] = []
    for error in errors:
        row_number = error.get("row_number", "?")
        field_name = str(error.get("field", "$"))
        code = str(error.get("code", "validation_error"))
        message = str(error.get("message", "validation failed"))
        chunks.append(f"row {row_number}, field {field_name}, code {code}: {message}")
    return "; ".join(chunks)


def contract_field_nullable(contract: ContractDefinition, field_name: str) -> bool:
    return field_name not in set(contract.required_fields) and field_name not in set(contract.key_fields)


def sqlalchemy_type_from_contract_field(contract: ContractDefinition, field_name: str) -> Any:
    property_schema = _contract_property_schema(contract, field_name)
    schema_type = _contract_schema_type(property_schema)

    if schema_type == "string":
        raw_format = property_schema.get("format")
        if isinstance(raw_format, str):
            normalized_format = raw_format.strip().lower()
            if normalized_format == "date-time":
                return DateTime(timezone=True)
            if normalized_format == "date":
                return Date()
            if normalized_format == "time":
                return Time()
            if normalized_format == "uuid":
                return PGUUID(as_uuid=False)
            if normalized_format in {"byte", "binary"}:
                return LargeBinary()
        return Text()
    if schema_type == "integer":
        return Integer()
    if schema_type == "number":
        return Numeric()
    if schema_type == "boolean":
        return Boolean()
    if schema_type == "object":
        return JSONB()
    if schema_type == "array":
        return JSONB()

    field_type = contract.field_types.get(field_name)
    normalized_field_type = (field_type or "").strip().lower()
    if normalized_field_type.startswith("array"):
        return JSONB()
    if normalized_field_type in {"json", "jsonb"}:
        return JSONB()
    if normalized_field_type == "uuid":
        return PGUUID(as_uuid=False)
    if normalized_field_type in {"bool", "boolean"}:
        return Boolean()
    if normalized_field_type == "date":
        return Date()
    if normalized_field_type == "time":
        return Time()
    if normalized_field_type in {
        "datetime",
        "timestamp",
        "timestampz",
        "timestamptz",
        "timestamp without time zone",
        "timestamp with time zone",
    }:
        return DateTime(timezone=True)
    if normalized_field_type in {
        "tinyint",
        "smallint",
        "int",
        "integer",
        "bigint",
        "serial",
        "bigserial",
    }:
        return Integer()
    if normalized_field_type in {
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
        return Numeric()
    if normalized_field_type in {"binary", "bytea", "blob"}:
        return LargeBinary()
    if normalized_field_type in {
        "string",
        "text",
        "char",
        "character",
        "varchar",
        "character varying",
        "citext",
    }:
        return Text()
    return JSONB()


def normalize_contract_row(
    row: Mapping[str, Any],
    contract: ContractDefinition,
    row_number: int,
) -> ContractRowValidationResult:
    errors: list[dict[str, Any]] = []
    normalized_row: dict[str, Any] = {}

    for field in contract.fields:
        if field not in row:
            errors.append(
                _build_validation_error(
                    row_number=row_number,
                    field=field,
                    code="missing_field",
                    message=f"Field '{field}' is absent in extracted row",
                )
            )
            continue

        raw_value = row[field]
        if raw_value is None and field in contract.required_fields:
            errors.append(
                _build_validation_error(
                    row_number=row_number,
                    field=field,
                    code="required_field",
                    message=f"Field '{field}' must not be null",
                )
            )
            normalized_row[field] = None
            continue

        try:
            normalized_row[field] = coerce_contract_value(
                raw_value,
                contract.field_types.get(field),
            )
        except (TypeError, ValueError) as exc:
            errors.append(
                _build_validation_error(
                    row_number=row_number,
                    field=field,
                    code="invalid_value",
                    message=str(exc),
                )
            )

    key_tuple = tuple(normalized_row.get(field) for field in contract.key_fields)
    if not errors and any(value is None for value in key_tuple):
        errors.append(
            _build_validation_error(
                row_number=row_number,
                field=",".join(contract.key_fields),
                code="null_key",
                message="Key fields must not contain null values",
            )
        )

    if not errors:
        schema_violations = validate_instance_against_schema(contract.schema_json, normalized_row)
        for violation in schema_violations:
            error_payload = _build_validation_error(
                row_number=row_number,
                field=violation.field,
                code=violation.code,
                message=violation.message,
            )
            if violation.constraint is not None:
                error_payload["constraint"] = violation.constraint
            if violation.actual_value is not None:
                error_payload["actual_value"] = violation.actual_value
            if violation.contract_title is not None:
                error_payload["contract_title"] = violation.contract_title
            if violation.contract_description is not None:
                error_payload["contract_description"] = violation.contract_description
            errors.append(error_payload)

    return ContractRowValidationResult(
        normalized_row=normalized_row,
        errors=errors,
    )


def build_contract_row_payload(
    normalized_row: Mapping[str, Any],
    contract: ContractDefinition,
) -> dict[str, Any]:
    payload = {
        field: normalize_json_value(normalized_row[field])
        for field in contract.fields
    }
    payload["row_hash"] = calculate_row_hash(
        normalized_row,
        contract.effective_hash_fields,
    )
    return payload


def _build_validation_error(
    *,
    row_number: int,
    field: str,
    code: str,
    message: str,
) -> dict[str, Any]:
    return {
        "row_number": row_number,
        "field": field,
        "code": code,
        "message": message,
    }


def _contract_property_schema(contract: ContractDefinition, field_name: str) -> Mapping[str, Any]:
    properties = contract.schema_json.get("properties")
    if not isinstance(properties, Mapping):
        return {}

    property_schema = properties.get(field_name)
    if not isinstance(property_schema, Mapping):
        return {}
    return property_schema


def _contract_schema_type(property_schema: Mapping[str, Any]) -> str | None:
    raw_type = property_schema.get("type")
    if isinstance(raw_type, str) and raw_type.strip():
        return raw_type.strip().lower()
    if isinstance(raw_type, list):
        for candidate in raw_type:
            if isinstance(candidate, str):
                normalized = candidate.strip().lower()
                if normalized and normalized != "null":
                    return normalized
    return None
