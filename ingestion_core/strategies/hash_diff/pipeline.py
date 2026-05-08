from __future__ import annotations

from dataclasses import dataclass
import gzip
import json
import logging
import os
from tempfile import NamedTemporaryFile
import uuid
from typing import Any, Mapping

from sqlalchemy import Column, MetaData, PrimaryKeyConstraint, Table, Text

from ingestion_core.adapters.object_store import ObjectStoreClient, ObjectStoreConfig
from ingestion_core.adapters.postgres import create_sqlalchemy_engine, ensure_schema, parse_table_name, reflect_table
from ingestion_core.contracts.runtime import (
    ContractValidationError,
    build_contract_row_payload as _build_contract_row_payload,
    coerce_contract_value as _coerce_contract_value,
    contract_field_nullable as _contract_field_nullable,
    normalize_contract_row as _normalize_contract_row,
    sqlalchemy_type_from_contract_field as _sqlalchemy_type_from_contract_field,
    summarize_validation_errors as _summarize_validation_errors,
)
from ingestion_core.contracts.types import ContractDefinition
from ingestion_core.strategies.hash_diff.engine import (
    HashDiffResult,
    _iter_source_row_batches,
    _validate_source_columns,
    run_hash_diff,
)

logger = logging.getLogger(__name__)


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
                    validation_result = _normalize_contract_row(
                        row=row,
                        contract=contract,
                        row_number=source_row_number,
                    )
                    row_errors = list(validation_result.errors)
                    normalized_row = validation_result.normalized_row
                    key_tuple = tuple(normalized_row.get(field) for field in contract.key_fields)
                    if not row_errors and key_tuple in seen_keys:
                        row_errors.append(
                            {
                                "row_number": source_row_number,
                                "field": ",".join(contract.key_fields),
                                "code": "duplicate_key",
                                "message": "Duplicate key detected inside extracted snapshot",
                            }
                        )

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
                    normalized_payload = _build_contract_row_payload(normalized_row, contract)
                    accepted_writer.write(
                        json.dumps(
                            normalized_payload,
                            ensure_ascii=True,
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
                f"Examples: {error_summary}",
                error_object_key=uploaded_error_key,
                manifest_key=manifest_object_key,
                accepted_object_key=uploaded_accepted_key,
                invalid_row_count=invalid_row_count,
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
