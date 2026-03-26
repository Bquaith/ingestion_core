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

from sqlalchemy import Boolean, Column, Date, DateTime, Integer, LargeBinary, MetaData, Numeric, PrimaryKeyConstraint, Table, Text, Time, text
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
from ingestion_core.postgres import create_sqlalchemy_engine, ensure_schema, parse_table_name, reflect_table, table_exists

logger = logging.getLogger(__name__)


class ContractValidationError(RuntimeError):
    """Raised when extracted rows do not satisfy the active contract."""


_VALIDATION_ERROR_PREVIEW_LIMIT = 5


@dataclass(frozen=True)
class ExtractSnapshotResult:
    object_key: str
    manifest_key: str
    row_count: int
    processed_batches: int
    source_read_seconds: float

    def to_dict(self) -> dict[str, Any]:
        return {
            "object_key": self.object_key,
            "manifest_key": self.manifest_key,
            "row_count": self.row_count,
            "processed_batches": self.processed_batches,
            "source_read_seconds": self.source_read_seconds,
        }


@dataclass(frozen=True)
class ValidationResult:
    validated_object_key: str
    error_object_key: str
    manifest_key: str
    valid_row_count: int
    invalid_row_count: int

    def to_dict(self) -> dict[str, Any]:
        return {
            "validated_object_key": self.validated_object_key,
            "error_object_key": self.error_object_key,
            "manifest_key": self.manifest_key,
            "valid_row_count": self.valid_row_count,
            "invalid_row_count": self.invalid_row_count,
        }


@dataclass(frozen=True)
class LandSnapshotResult:
    accepted_object_key: str
    manifest_key: str
    row_count: int

    def to_dict(self) -> dict[str, Any]:
        return {
            "accepted_object_key": self.accepted_object_key,
            "manifest_key": self.manifest_key,
            "row_count": self.row_count,
        }


@dataclass(frozen=True)
class LoadRawResult:
    raw_table: str
    loaded_row_count: int

    def to_dict(self) -> dict[str, Any]:
        return {
            "raw_table": self.raw_table,
            "loaded_row_count": self.loaded_row_count,
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


def _ensure_raw_table(
    target_engine,
    target_schema: str,
    target_table_name: str,
    contract: ContractDefinition,
) -> Table:
    ensure_schema(target_engine, target_schema)

    if not table_exists(target_engine, target_schema, target_table_name):
        metadata = MetaData()
        business_columns = [
            Column(
                field,
                _sqlalchemy_type_from_contract_field(contract, field),
                nullable=_contract_field_nullable(contract, field),
            )
            for field in contract.fields
        ]
        technical_columns = [
            Column("_run_id", Text, nullable=False),
            Column("_record_index", Integer, nullable=False),
            Column("_row_hash", Text, nullable=False),
            Column("_loaded_at", DateTime(timezone=True), nullable=False),
            Column("_contract_version", Text, nullable=False),
            Column("_contract_checksum", Text, nullable=False),
            Column("_source_object_key", Text, nullable=False),
        ]
        table = Table(
            target_table_name,
            metadata,
            *business_columns,
            *technical_columns,
            PrimaryKeyConstraint("_run_id", *contract.key_fields, name=f"pk_{target_table_name}"[:63]),
            schema=target_schema,
        )
        metadata.create_all(target_engine, tables=[table], checkfirst=True)

    raw_table = reflect_table(target_engine, target_schema, target_table_name)
    required_columns = set(contract.fields) | {
        "_run_id",
        "_record_index",
        "_row_hash",
        "_loaded_at",
        "_contract_version",
        "_contract_checksum",
        "_source_object_key",
    }
    missing_columns = sorted(required_columns - set(raw_table.columns.keys()))
    if missing_columns:
        raise ValueError(f"Raw table is missing required columns: {missing_columns}")
    _validate_source_columns(
        source_table=raw_table,
        fields=contract.fields,
        field_types=contract.field_types,
    )

    return raw_table


def _write_manifest(
    object_store: ObjectStoreClient,
    manifest_key: str,
    payload: Mapping[str, Any],
) -> str:
    return object_store.put_json(manifest_key, dict(payload))


def extract_source_snapshot(
    source_dsn: str,
    source_table: str,
    contract: ContractDefinition,
    object_store_config: ObjectStoreConfig,
    extracted_object_key: str,
    manifest_key: str,
    source_batch_size: int = 1000,
) -> ExtractSnapshotResult:
    if source_batch_size <= 0:
        raise ValueError("source_batch_size must be greater than zero")

    source_engine = create_sqlalchemy_engine(source_dsn)
    object_store = ObjectStoreClient(object_store_config)
    row_count = 0
    processed_batches = 0
    source_read_seconds = 0.0

    with NamedTemporaryFile(suffix=".ndjson.gz", delete=False) as temp_file:
        temp_path = temp_file.name

    try:
        source_schema, source_name = parse_table_name(source_table)
        source_table_obj = reflect_table(source_engine, source_schema, source_name)
        _validate_source_columns(
            source_table=source_table_obj,
            fields=contract.fields,
            field_types=contract.field_types,
        )

        with gzip.open(temp_path, mode="wt", encoding="utf-8") as gz_writer:
            for source_batch, fetch_seconds in _iter_source_row_batches(
                source_engine=source_engine,
                source_table=source_table_obj,
                fields=contract.fields,
                key_fields=contract.key_fields,
                source_batch_size=source_batch_size,
            ):
                processed_batches += 1
                source_read_seconds += fetch_seconds
                row_count += len(source_batch)
                for row in source_batch:
                    gz_writer.write(
                        json.dumps(
                            {field: _normalize_json_value(row[field]) for field in contract.fields},
                            ensure_ascii=True,
                            default=_json_default,
                        )
                    )
                    gz_writer.write("\n")

        object_key = object_store.upload_file(
            temp_path,
            extracted_object_key,
            content_type="application/x-ndjson",
            content_encoding="gzip",
        )
        manifest_object_key = _write_manifest(
            object_store,
            manifest_key,
            {
                "stage": "extract",
                "source_table": source_table,
                "row_count": row_count,
                "processed_batches": processed_batches,
                "source_read_seconds": source_read_seconds,
                "contract_id": contract.contract_id,
                "contract_version": contract.version,
                "extracted_object_key": object_key,
            },
        )
        return ExtractSnapshotResult(
            object_key=object_key,
            manifest_key=manifest_object_key,
            row_count=row_count,
            processed_batches=processed_batches,
            source_read_seconds=source_read_seconds,
        )
    finally:
        source_engine.dispose()
        if os.path.exists(temp_path):
            os.unlink(temp_path)


def validate_extracted_snapshot(
    contract: ContractDefinition,
    object_store_config: ObjectStoreConfig,
    extracted_object_key: str,
    validated_object_key: str,
    error_object_key: str,
    manifest_key: str,
) -> ValidationResult:
    object_store = ObjectStoreClient(object_store_config)
    valid_row_count = 0
    invalid_row_count = 0
    seen_keys: set[tuple[Any, ...]] = set()
    preview_errors: list[dict[str, Any]] = []

    logger.info(
        "Validation started for contract_id=%s contract_version=%s extracted_object_key=%s",
        contract.contract_id,
        contract.version,
        extracted_object_key,
    )

    with NamedTemporaryFile(suffix=".accepted.ndjson.gz", delete=False) as accepted_temp_file:
        accepted_temp_path = accepted_temp_file.name
    with NamedTemporaryFile(suffix=".errors.ndjson.gz", delete=False) as error_temp_file:
        error_temp_path = error_temp_file.name

    try:
        with object_store.open_gzip_text_reader(extracted_object_key) as reader, \
            gzip.open(accepted_temp_path, mode="wt", encoding="utf-8") as accepted_writer, \
            gzip.open(error_temp_path, mode="wt", encoding="utf-8") as error_writer:
            for row_number, raw_line in enumerate(reader, start=1):
                line = raw_line.strip()
                if not line:
                    continue

                row_payload = json.loads(line)
                row_errors: list[dict[str, Any]] = []
                normalized_row: dict[str, Any] = {}

                for field in contract.fields:
                    if field not in row_payload:
                        row_errors.append(
                            {
                                "row_number": row_number,
                                "field": field,
                                "code": "missing_field",
                                "message": f"Field '{field}' is absent in extracted row",
                            }
                        )
                        continue

                    raw_value = row_payload[field]
                    if raw_value is None and field in contract.required_fields:
                        row_errors.append(
                            {
                                "row_number": row_number,
                                "field": field,
                                "code": "required_field",
                                "message": f"Field '{field}' must not be null",
                            }
                        )
                        normalized_row[field] = None
                        continue

                    try:
                        normalized_row[field] = _coerce_contract_value(raw_value, contract.field_types.get(field))
                    except (TypeError, ValueError) as exc:
                        row_errors.append(
                            {
                                "row_number": row_number,
                                "field": field,
                                "code": "invalid_value",
                                "message": str(exc),
                            }
                        )

                key_tuple = tuple(normalized_row.get(field) for field in contract.key_fields)
                if not row_errors and any(value is None for value in key_tuple):
                    row_errors.append(
                        {
                            "row_number": row_number,
                            "field": ",".join(contract.key_fields),
                            "code": "null_key",
                            "message": "Key fields must not contain null values",
                        }
                    )
                if not row_errors and key_tuple in seen_keys:
                    row_errors.append(
                        {
                            "row_number": row_number,
                            "field": ",".join(contract.key_fields),
                            "code": "duplicate_key",
                            "message": "Duplicate key detected inside extracted snapshot",
                        }
                    )

                if not row_errors:
                    schema_violations = validate_instance_against_schema(contract.schema_json, normalized_row)
                    for violation in schema_violations:
                        error_payload = {
                            "row_number": row_number,
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
                            row_number,
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
                normalized_payload["row_hash"] = calculate_row_hash(normalized_row, contract.effective_hash_fields)
                accepted_writer.write(json.dumps(normalized_payload, ensure_ascii=True, default=_json_default))
                accepted_writer.write("\n")
                valid_row_count += 1

        validated_key = object_store.upload_file(
            accepted_temp_path,
            validated_object_key,
            content_type="application/x-ndjson",
            content_encoding="gzip",
        )
        error_key = object_store.upload_file(
            error_temp_path,
            error_object_key,
            content_type="application/x-ndjson",
            content_encoding="gzip",
        )
        manifest_object_key = _write_manifest(
            object_store,
            manifest_key,
            {
                "stage": "validate",
                "validated_object_key": validated_key,
                "error_object_key": error_key,
                "valid_row_count": valid_row_count,
                "invalid_row_count": invalid_row_count,
                "sample_errors": preview_errors,
                "contract_id": contract.contract_id,
                "contract_version": contract.version,
            },
        )
        if invalid_row_count > 0:
            error_summary = _summarize_validation_errors(preview_errors)
            logger.error(
                "Validation failed for contract_id=%s invalid_row_count=%s error_report=%s examples=%s",
                contract.contract_id,
                invalid_row_count,
                error_key,
                error_summary,
            )
            raise ContractValidationError(
                "Validation failed for contract "
                f"{contract.contract_id}: {invalid_row_count} invalid rows. "
                f"Error report: {error_key}. "
                f"Examples: {error_summary}"
            )
        logger.info(
            "Validation succeeded for contract_id=%s valid_row_count=%s validated_object_key=%s error_object_key=%s",
            contract.contract_id,
            valid_row_count,
            validated_key,
            error_key,
        )
        return ValidationResult(
            validated_object_key=validated_key,
            error_object_key=error_key,
            manifest_key=manifest_object_key,
            valid_row_count=valid_row_count,
            invalid_row_count=invalid_row_count,
        )
    finally:
        if os.path.exists(accepted_temp_path):
            os.unlink(accepted_temp_path)
        if os.path.exists(error_temp_path):
            os.unlink(error_temp_path)


def land_validated_snapshot(
    object_store_config: ObjectStoreConfig,
    staged_validated_object_key: str,
    accepted_object_key: str,
    manifest_key: str,
    row_count: int,
) -> LandSnapshotResult:
    object_store = ObjectStoreClient(object_store_config)
    accepted_key = object_store.copy_object(staged_validated_object_key, accepted_object_key)
    manifest_object_key = _write_manifest(
        object_store,
        manifest_key,
        {
            "stage": "land",
            "accepted_object_key": accepted_key,
            "row_count": row_count,
        },
    )
    return LandSnapshotResult(
        accepted_object_key=accepted_key,
        manifest_key=manifest_object_key,
        row_count=row_count,
    )


def load_raw_snapshot(
    target_dsn: str,
    target_table_raw: str,
    contract: ContractDefinition,
    object_store_config: ObjectStoreConfig,
    accepted_object_key: str,
    run_id: str,
    raw_load_batch_size: int = 1000,
) -> LoadRawResult:
    if raw_load_batch_size <= 0:
        raise ValueError("raw_load_batch_size must be greater than zero")

    target_engine = create_sqlalchemy_engine(target_dsn)
    object_store = ObjectStoreClient(object_store_config)
    loaded_at = datetime.now(timezone.utc)

    try:
        raw_schema, raw_name = parse_table_name(target_table_raw)
        raw_table = _ensure_raw_table(
            target_engine=target_engine,
            target_schema=raw_schema,
            target_table_name=raw_name,
            contract=contract,
        )

        loaded_row_count = 0
        with target_engine.begin() as conn:
            conn.execute(raw_table.delete().where(raw_table.c._run_id == run_id))

            rows_buffer: list[dict[str, Any]] = []
            with object_store.open_gzip_text_reader(accepted_object_key) as reader:
                for row_number, raw_line in enumerate(reader, start=1):
                    line = raw_line.strip()
                    if not line:
                        continue
                    row_payload = json.loads(line)
                    row_hash = str(row_payload.pop("row_hash"))
                    inserted_row = {
                        field: _coerce_contract_value(row_payload.get(field), contract.field_types.get(field))
                        for field in contract.fields
                    }
                    inserted_row.update(
                        {
                            "_run_id": run_id,
                            "_record_index": row_number,
                            "_row_hash": row_hash,
                            "_loaded_at": loaded_at,
                            "_contract_version": contract.version,
                            "_contract_checksum": contract.checksum,
                            "_source_object_key": accepted_object_key,
                        }
                    )
                    rows_buffer.append(inserted_row)
                    if len(rows_buffer) >= raw_load_batch_size:
                        conn.execute(raw_table.insert(), rows_buffer)
                        loaded_row_count += len(rows_buffer)
                        rows_buffer = []

                if rows_buffer:
                    conn.execute(raw_table.insert(), rows_buffer)
                    loaded_row_count += len(rows_buffer)

        return LoadRawResult(
            raw_table=f"{raw_schema}.{raw_name}",
            loaded_row_count=loaded_row_count,
        )
    finally:
        target_engine.dispose()


def _build_merge_snapshot_table_name(target_table_name: str) -> str:
    suffix = uuid.uuid4().hex[:10]
    return f"_hd_snapshot_{target_table_name}_{suffix}"[:63]


def merge_raw_snapshot_to_curated(
    target_dsn: str,
    target_table_raw: str,
    target_table_curated: str,
    contract: ContractDefinition,
    run_id: str,
    source_batch_size: int = 1000,
    upsert_batch_size: int = 1000,
) -> HashDiffResult:
    target_engine = create_sqlalchemy_engine(target_dsn)

    try:
        raw_schema, raw_name = parse_table_name(target_table_raw)
        raw_table = reflect_table(target_engine, raw_schema, raw_name)
        missing_columns = sorted(set(contract.fields) - set(raw_table.columns.keys()))
        if missing_columns:
            raise ValueError(f"Raw table is missing required columns for merge: {missing_columns}")

        merge_schema = raw_schema
        merge_table_name = _build_merge_snapshot_table_name(raw_name)
        projected_columns = [f'"{field}"' for field in contract.fields]
        projected_columns.append('"_row_hash" AS "row_hash"')
        field_projection = ", ".join(projected_columns)
        create_snapshot_sql = (
            f'CREATE TABLE "{merge_schema}"."{merge_table_name}" AS '
            f'SELECT {field_projection} '
            f'FROM "{raw_schema}"."{raw_name}" '
            f'WHERE "_run_id" = :run_id'
        )

        with target_engine.begin() as conn:
            conn.execute(text(create_snapshot_sql), {"run_id": run_id})

        snapshot_source_table = f"{merge_schema}.{merge_table_name}"
        try:
            return run_hash_diff(
                source_dsn=target_dsn,
                source_table=snapshot_source_table,
                target_dsn=target_dsn,
                target_table_curated=target_table_curated,
                contract=contract,
                source_batch_size=source_batch_size,
                upsert_batch_size=upsert_batch_size,
            )
        finally:
            with target_engine.begin() as conn:
                conn.execute(text(f'DROP TABLE IF EXISTS "{merge_schema}"."{merge_table_name}"'))
    finally:
        target_engine.dispose()
