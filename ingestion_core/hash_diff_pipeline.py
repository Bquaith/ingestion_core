from __future__ import annotations

import base64
from dataclasses import dataclass
from datetime import date, datetime, time, timezone
from decimal import Decimal, InvalidOperation
import gzip
import json
import logging
import os
from tempfile import NamedTemporaryFile
import uuid
from typing import Any, Mapping

from sqlalchemy import Boolean, Column, Date, DateTime, Integer, LargeBinary, MetaData, Numeric, PrimaryKeyConstraint, Table, Text, Time
from sqlalchemy.dialects.postgresql import JSONB, UUID as PGUUID

from ingestion_core.contract_schema_validation import validate_instance_against_schema
from ingestion_core.hash_diff import (
    ContractDefinition,
    HashDiffResult,
    _iter_source_row_batches,
    _validate_source_columns,
    run_hash_diff,
)
from ingestion_core.hashing import calculate_row_hash
from ingestion_core.object_store import ObjectStoreClient, ObjectStoreConfig
from ingestion_core.postgres import create_sqlalchemy_engine, ensure_schema, parse_table_name, reflect_table

logger = logging.getLogger(__name__)


class ContractValidationError(RuntimeError):
    """Raised when extracted rows do not satisfy the active contract."""


_VALIDATION_ERROR_PREVIEW_LIMIT = 5


@dataclass(frozen=True)
class ExtractValidateLandResult:
    accepted_object_key: str | None
    error_object_key: str
    manifest_key: str
    source_row_count: int
    valid_row_count: int
    invalid_row_count: int
    processed_batches: int
    source_read_seconds: float

    def to_dict(self) -> dict[str, Any]:
        return {
            "accepted_object_key": self.accepted_object_key,
            "error_object_key": self.error_object_key,
            "manifest_key": self.manifest_key,
            "source_row_count": self.source_row_count,
            "valid_row_count": self.valid_row_count,
            "invalid_row_count": self.invalid_row_count,
            "processed_batches": self.processed_batches,
            "source_read_seconds": self.source_read_seconds,
        }


def _json_default(value: Any) -> Any:
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


def _normalize_json_value(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(key): _normalize_json_value(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_normalize_json_value(item) for item in value]
    if isinstance(value, (datetime, date, time, Decimal, bytes, bytearray, memoryview)):
        return _json_default(value)
    return value


def _parse_iso_datetime(value: str) -> datetime:
    trimmed = value.strip()
    if trimmed.endswith("Z"):
        trimmed = f"{trimmed[:-1]}+00:00"
    return datetime.fromisoformat(trimmed)


def _coerce_contract_value(value: Any, field_type: str | None) -> Any:
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
            return _parse_iso_datetime(value)
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


def _contract_field_nullable(contract: ContractDefinition, field_name: str) -> bool:
    return field_name not in set(contract.required_fields) and field_name not in set(contract.key_fields)


def _summarize_validation_errors(errors: list[dict[str, Any]]) -> str:
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


def _sqlalchemy_type_from_contract_field(contract: ContractDefinition, field_name: str) -> Any:
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


def _create_merge_staging_table(
    target_engine,
    target_schema: str,
    target_table_name: str,
    contract: ContractDefinition,
) -> Table:
    ensure_schema(target_engine, target_schema)
    metadata = MetaData()
    business_columns = [
        Column(
            field,
            _sqlalchemy_type_from_contract_field(contract, field),
            nullable=_contract_field_nullable(contract, field),
        )
        for field in contract.fields
    ]
    table = Table(
        target_table_name,
        metadata,
        *business_columns,
        Column("row_hash", Text, nullable=False),
        PrimaryKeyConstraint(*contract.key_fields, name=f"pk_{target_table_name}"[:63]),
        schema=target_schema,
    )
    metadata.create_all(target_engine, tables=[table], checkfirst=False)
    return reflect_table(target_engine, target_schema, target_table_name)


def _drop_staging_table(target_engine, staging_table: Table) -> None:
    staging_table.drop(target_engine, checkfirst=True)


def _write_manifest(
    object_store: ObjectStoreClient,
    manifest_key: str,
    payload: Mapping[str, Any],
) -> str:
    return object_store.put_json(manifest_key, dict(payload))


def extract_validate_land_snapshot(
    source_dsn: str,
    source_table: str,
    contract: ContractDefinition,
    object_store_config: ObjectStoreConfig,
    accepted_object_key: str,
    error_object_key: str,
    manifest_key: str,
    source_batch_size: int = 1000,
) -> ExtractValidateLandResult:
    if source_batch_size <= 0:
        raise ValueError("source_batch_size must be greater than zero")

    source_engine = create_sqlalchemy_engine(source_dsn)
    object_store = ObjectStoreClient(object_store_config)
    source_row_count = 0
    valid_row_count = 0
    invalid_row_count = 0
    processed_batches = 0
    source_read_seconds = 0.0
    source_row_number = 0
    seen_keys: set[tuple[Any, ...]] = set()
    preview_errors: list[dict[str, Any]] = []

    with NamedTemporaryFile(suffix=".accepted.ndjson.gz", delete=False) as accepted_temp_file:
        accepted_temp_path = accepted_temp_file.name
    with NamedTemporaryFile(suffix=".errors.ndjson.gz", delete=False) as error_temp_file:
        error_temp_path = error_temp_file.name

    try:
        source_schema, source_name = parse_table_name(source_table)
        source_table_obj = reflect_table(source_engine, source_schema, source_name)
        _validate_source_columns(
            source_table=source_table_obj,
            fields=contract.fields,
            field_types=contract.field_types,
        )

        logger.info(
            "Extract-validate-land started for contract_id=%s contract_version=%s source_table=%s",
            contract.contract_id,
            contract.version,
            source_table,
        )

        with gzip.open(accepted_temp_path, mode="wt", encoding="utf-8") as accepted_writer, gzip.open(
            error_temp_path,
            mode="wt",
            encoding="utf-8",
        ) as error_writer:
            for source_batch, fetch_seconds in _iter_source_row_batches(
                source_engine=source_engine,
                source_table=source_table_obj,
                fields=contract.fields,
                key_fields=contract.key_fields,
                source_batch_size=source_batch_size,
            ):
                processed_batches += 1
                source_read_seconds += fetch_seconds
                source_row_count += len(source_batch)

                for row in source_batch:
                    source_row_number += 1
                    row_errors: list[dict[str, Any]] = []
                    normalized_row: dict[str, Any] = {}

                    for field in contract.fields:
                        if field not in row:
                            row_errors.append(
                                {
                                    "row_number": source_row_number,
                                    "field": field,
                                    "code": "missing_field",
                                    "message": f"Field '{field}' is absent in extracted row",
                                }
                            )
                            continue

                        raw_value = row[field]
                        if raw_value is None and field in contract.required_fields:
                            row_errors.append(
                                {
                                    "row_number": source_row_number,
                                    "field": field,
                                    "code": "required_field",
                                    "message": f"Field '{field}' must not be null",
                                }
                            )
                            normalized_row[field] = None
                            continue

                        try:
                            normalized_row[field] = _coerce_contract_value(
                                raw_value,
                                contract.field_types.get(field),
                            )
                        except (TypeError, ValueError) as exc:
                            row_errors.append(
                                {
                                    "row_number": source_row_number,
                                    "field": field,
                                    "code": "invalid_value",
                                    "message": str(exc),
                                }
                            )

                    key_tuple = tuple(normalized_row.get(field) for field in contract.key_fields)
                    if not row_errors and any(value is None for value in key_tuple):
                        row_errors.append(
                            {
                                "row_number": source_row_number,
                                "field": ",".join(contract.key_fields),
                                "code": "null_key",
                                "message": "Key fields must not contain null values",
                            }
                        )
                    if not row_errors and key_tuple in seen_keys:
                        row_errors.append(
                            {
                                "row_number": source_row_number,
                                "field": ",".join(contract.key_fields),
                                "code": "duplicate_key",
                                "message": "Duplicate key detected inside extracted snapshot",
                            }
                        )

                    if not row_errors:
                        schema_violations = validate_instance_against_schema(contract.schema_json, normalized_row)
                        for violation in schema_violations:
                            error_payload = {
                                "row_number": source_row_number,
                                "field": violation.field,
                                "code": violation.code,
                                "message": violation.message,
                            }
                            if violation.constraint is not None:
                                error_payload["constraint"] = violation.constraint
                            if violation.actual_value is not None:
                                error_payload["actual_value"] = violation.actual_value
                            if violation.contract_title is not None:
                                error_payload["contract_title"] = violation.contract_title
                            if violation.contract_description is not None:
                                error_payload["contract_description"] = violation.contract_description
                            row_errors.append(error_payload)

                    if row_errors:
                        invalid_row_count += 1
                        if len(preview_errors) < _VALIDATION_ERROR_PREVIEW_LIMIT:
                            remaining_slots = _VALIDATION_ERROR_PREVIEW_LIMIT - len(preview_errors)
                            preview_errors.extend(dict(error) for error in row_errors[:remaining_slots])
                        if invalid_row_count <= _VALIDATION_ERROR_PREVIEW_LIMIT:
                            logger.warning(
                                "Validation errors for contract_id=%s row=%s details=%s",
                                contract.contract_id,
                                source_row_number,
                                row_errors,
                            )
                        for error in row_errors:
                            error_writer.write(json.dumps(error, ensure_ascii=True))
                            error_writer.write("\n")
                        continue

                    seen_keys.add(key_tuple)
                    normalized_payload = {
                        field: _normalize_json_value(normalized_row[field]) for field in contract.fields
                    }
                    normalized_payload["row_hash"] = calculate_row_hash(
                        normalized_row,
                        contract.effective_hash_fields,
                    )
                    accepted_writer.write(
                        json.dumps(
                            normalized_payload,
                            ensure_ascii=True,
                            default=_json_default,
                        )
                    )
                    accepted_writer.write("\n")
                    valid_row_count += 1

        uploaded_error_key = object_store.upload_file(
            error_temp_path,
            error_object_key,
            content_type="application/x-ndjson",
            content_encoding="gzip",
        )

        uploaded_accepted_key: str | None = None
        if invalid_row_count == 0:
            uploaded_accepted_key = object_store.upload_file(
                accepted_temp_path,
                accepted_object_key,
                content_type="application/x-ndjson",
                content_encoding="gzip",
            )

        manifest_object_key = _write_manifest(
            object_store,
            manifest_key,
            {
                "stage": "extract_validate_land",
                "source_table": source_table,
                "source_row_count": source_row_count,
                "valid_row_count": valid_row_count,
                "invalid_row_count": invalid_row_count,
                "processed_batches": processed_batches,
                "source_read_seconds": source_read_seconds,
                "contract_id": contract.contract_id,
                "contract_version": contract.version,
                "accepted_object_key": uploaded_accepted_key,
                "error_object_key": uploaded_error_key,
                "sample_errors": preview_errors,
            },
        )

        if invalid_row_count > 0:
            error_summary = _summarize_validation_errors(preview_errors)
            logger.error(
                "Validation failed for contract_id=%s invalid_row_count=%s error_report=%s examples=%s",
                contract.contract_id,
                invalid_row_count,
                uploaded_error_key,
                error_summary,
            )
            raise ContractValidationError(
                "Validation failed for contract "
                f"{contract.contract_id}: {invalid_row_count} invalid rows. "
                f"Error report: {uploaded_error_key}. "
                f"Examples: {error_summary}"
            )

        logger.info(
            "Extract-validate-land succeeded for contract_id=%s valid_row_count=%s accepted_object_key=%s",
            contract.contract_id,
            valid_row_count,
            uploaded_accepted_key,
        )
        return ExtractValidateLandResult(
            accepted_object_key=uploaded_accepted_key,
            error_object_key=uploaded_error_key,
            manifest_key=manifest_object_key,
            source_row_count=source_row_count,
            valid_row_count=valid_row_count,
            invalid_row_count=invalid_row_count,
            processed_batches=processed_batches,
            source_read_seconds=source_read_seconds,
        )
    finally:
        source_engine.dispose()
        if os.path.exists(accepted_temp_path):
            os.unlink(accepted_temp_path)
        if os.path.exists(error_temp_path):
            os.unlink(error_temp_path)


def _build_merge_staging_table_name(target_table_name: str) -> str:
    suffix = uuid.uuid4().hex[:10]
    return f"_hd_merge_{target_table_name}_{suffix}"[:63]


def merge_accepted_snapshot_to_curated(
    target_dsn: str,
    target_table_curated: str,
    contract: ContractDefinition,
    object_store_config: ObjectStoreConfig,
    accepted_object_key: str,
    merge_load_batch_size: int = 1000,
    source_batch_size: int = 1000,
    upsert_batch_size: int = 1000,
) -> HashDiffResult:
    if merge_load_batch_size <= 0:
        raise ValueError("merge_load_batch_size must be greater than zero")

    target_engine = create_sqlalchemy_engine(target_dsn)
    object_store = ObjectStoreClient(object_store_config)

    try:
        merge_schema, target_table_name = parse_table_name(target_table_curated)
        merge_table_name = _build_merge_staging_table_name(target_table_name)
        merge_table = _create_merge_staging_table(
            target_engine=target_engine,
            target_schema=merge_schema,
            target_table_name=merge_table_name,
            contract=contract,
        )

        try:
            with target_engine.begin() as conn, object_store.open_gzip_text_reader(accepted_object_key) as reader:
                rows_buffer: list[dict[str, Any]] = []
                for raw_line in reader:
                    line = raw_line.strip()
                    if not line:
                        continue

                    row_payload = json.loads(line)
                    row_hash = str(row_payload.pop("row_hash"))
                    inserted_row = {
                        field: _coerce_contract_value(row_payload.get(field), contract.field_types.get(field))
                        for field in contract.fields
                    }
                    inserted_row["row_hash"] = row_hash
                    rows_buffer.append(inserted_row)

                    if len(rows_buffer) >= merge_load_batch_size:
                        conn.execute(merge_table.insert(), rows_buffer)
                        rows_buffer = []

                if rows_buffer:
                    conn.execute(merge_table.insert(), rows_buffer)

            return run_hash_diff(
                source_dsn=target_dsn,
                source_table=f"{merge_schema}.{merge_table_name}",
                target_dsn=target_dsn,
                target_table_curated=target_table_curated,
                contract=contract,
                source_batch_size=source_batch_size,
                upsert_batch_size=upsert_batch_size,
            )
        finally:
            _drop_staging_table(target_engine=target_engine, staging_table=merge_table)
    finally:
        target_engine.dispose()
