from __future__ import annotations

import gzip
import io
import json
from datetime import datetime, timezone
from decimal import Decimal

import pytest
from sqlalchemy import Date, DateTime, Integer, Text, create_engine, text
from sqlalchemy.dialects.postgresql import JSONB, UUID as PGUUID

from ingestion_core.adapters.object_store import ObjectStoreConfig
from ingestion_core.contracts.runtime import (
    ContractValidationError,
    build_contract_row_payload,
    coerce_contract_value,
    normalize_contract_row,
    sqlalchemy_type_from_contract_field,
    summarize_validation_errors,
)
from ingestion_core.contracts.types import ContractDefinition
import ingestion_core.strategies.hash_diff.pipeline as hash_diff_pipeline_module
from ingestion_core.strategies.hash_diff.pipeline import (
    extract_validate_land_snapshot,
    merge_accepted_snapshot_to_curated,
)


class DummyEngine:
    def dispose(self) -> None:
        return None


class FakeUploadObjectStoreClient:
    uploads: dict[str, bytes] = {}
    json_payloads: dict[str, dict] = {}

    def __init__(self, config: ObjectStoreConfig) -> None:
        self.config = config

    @classmethod
    def reset(cls) -> None:
        cls.uploads = {}
        cls.json_payloads = {}

    def upload_file(
        self,
        file_path: str,
        key: str,
        content_type: str | None = None,
        content_encoding: str | None = None,
    ) -> str:
        del content_type, content_encoding
        with open(file_path, "rb") as source:
            type(self).uploads[key] = source.read()
        return key

    def put_json(self, key: str, payload: dict[str, object]) -> str:
        type(self).json_payloads[key] = dict(payload)
        return key


class FakeReaderObjectStoreClient:
    payloads: dict[str, bytes] = {}

    def __init__(self, config: ObjectStoreConfig) -> None:
        self.config = config

    def open_gzip_text_reader(self, key: str):
        raw_payload = type(self).payloads[key]
        gzip_stream = gzip.GzipFile(fileobj=io.BytesIO(raw_payload), mode="rb")
        return io.TextIOWrapper(gzip_stream, encoding="utf-8")


def _build_contract() -> ContractDefinition:
    return ContractDefinition.from_registry_payload(
        {
            "contract_id": "orders-contract",
            "target_layer": "curated",
            "version": "1.0.0",
            "checksum": "checksum-v1",
            "schema_json": {
                "type": "object",
                "properties": {
                    "id": {"type": "integer"},
                    "status": {"type": "string"},
                },
                "required": ["id"],
                "additionalProperties": False,
                "x-primaryKey": ["id"],
            },
            "fields": ["id", "status"],
            "field_types": {"id": "integer", "status": "string"},
            "required_fields": ["id"],
            "primary_keys": ["id"],
            "business_keys": [],
            "hash_keys": ["status"],
        }
    )


def _read_gzip_ndjson(payload: bytes) -> list[dict[str, object]]:
    with gzip.GzipFile(fileobj=io.BytesIO(payload), mode="rb") as gzip_stream:
        content = gzip_stream.read().decode("utf-8")
    return [json.loads(line) for line in content.splitlines() if line.strip()]


def test_coerce_contract_value_parses_decimal_and_timestamp() -> None:
    coerced_decimal = coerce_contract_value("10.50", "decimal")
    coerced_timestamp = coerce_contract_value("2025-01-01T10:00:00Z", "timestamp")

    assert coerced_decimal == Decimal("10.50")
    assert coerced_timestamp == datetime(2025, 1, 1, 10, 0, 0, tzinfo=timezone.utc)


def test_coerce_contract_value_rejects_invalid_boolean() -> None:
    with pytest.raises(ValueError, match="expected boolean value"):
        coerce_contract_value("maybe", "boolean")


def test_object_store_normalize_key_is_idempotent_for_prefixed_keys() -> None:
    config = ObjectStoreConfig(bucket="landing", prefix="accepted")

    assert config.normalize_key("accepted/sales/orders/file.json") == "accepted/sales/orders/file.json"


def test_summarize_validation_errors_includes_row_field_code_and_message() -> None:
    summary = summarize_validation_errors(
        [
            {
                "row_number": 7,
                "field": "gender",
                "code": "schema.enum",
                "message": "Value must be one of the contract enum values",
            }
        ]
    )

    assert summary == "row 7, field gender, code schema.enum: Value must be one of the contract enum values"


def test_sqlalchemy_type_from_contract_field_uses_schema_formats_and_json_containers() -> None:
    contract = ContractDefinition.from_registry_payload(
        {
            "contract_id": "c1",
            "target_layer": "raw",
            "version": "1",
            "checksum": "abc",
            "schema_json": {
                "type": "object",
                "properties": {
                    "id": {"type": "string", "format": "uuid"},
                    "created_at": {"type": "string", "format": "date-time"},
                    "birthday": {"type": "string", "format": "date"},
                    "amount": {"type": "integer"},
                    "payload": {"type": "object"},
                    "tags": {"type": "array", "items": {"type": "string"}},
                    "name": {"type": "string"},
                },
                "required": ["id"],
            },
            "fields": ["id", "created_at", "birthday", "amount", "payload", "tags", "name"],
            "field_types": {
                "id": "uuid",
                "created_at": "timestamp",
                "birthday": "date",
                "amount": "integer",
                "payload": "json",
                "tags": "array",
                "name": "string",
            },
            "required_fields": ["id"],
            "primary_keys": ["id"],
            "business_keys": [],
            "hash_keys": [],
        }
    )

    assert isinstance(sqlalchemy_type_from_contract_field(contract, "id"), PGUUID)
    assert isinstance(sqlalchemy_type_from_contract_field(contract, "created_at"), DateTime)
    assert isinstance(sqlalchemy_type_from_contract_field(contract, "birthday"), Date)
    assert isinstance(sqlalchemy_type_from_contract_field(contract, "amount"), Integer)
    assert isinstance(sqlalchemy_type_from_contract_field(contract, "payload"), JSONB)
    assert isinstance(sqlalchemy_type_from_contract_field(contract, "tags"), JSONB)
    assert isinstance(sqlalchemy_type_from_contract_field(contract, "name"), Text)


def test_normalize_contract_row_reports_required_field_error() -> None:
    contract = _build_contract()

    result = normalize_contract_row(
        row={"id": None, "status": "NEW"},
        contract=contract,
        row_number=3,
    )

    assert result.normalized_row["id"] is None
    assert result.errors == [
        {
            "row_number": 3,
            "field": "id",
            "code": "required_field",
            "message": "Field 'id' must not be null",
        }
    ]


def test_build_contract_row_payload_normalizes_values_and_adds_row_hash() -> None:
    contract = _build_contract()

    payload = build_contract_row_payload(
        normalized_row={
            "id": 1,
            "status": "NEW",
        },
        contract=contract,
    )

    assert payload["id"] == 1
    assert payload["status"] == "NEW"
    assert isinstance(payload["row_hash"], str)
    assert payload["row_hash"]


def test_extract_validate_land_snapshot_writes_only_accepted_and_error_artifacts(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    contract = _build_contract()
    FakeUploadObjectStoreClient.reset()

    monkeypatch.setattr(hash_diff_pipeline_module, "create_sqlalchemy_engine", lambda _: DummyEngine())
    monkeypatch.setattr(hash_diff_pipeline_module, "parse_table_name", lambda _: ("public", "orders"))
    monkeypatch.setattr(hash_diff_pipeline_module, "reflect_table", lambda *args, **kwargs: object())
    monkeypatch.setattr(hash_diff_pipeline_module, "_validate_source_columns", lambda *args, **kwargs: None)
    monkeypatch.setattr(
        hash_diff_pipeline_module,
        "_iter_source_row_batches",
        lambda **kwargs: iter(
            [
                (
                    [
                        {"id": 1, "status": "NEW"},
                        {"id": 2, "status": "PAID"},
                    ],
                    0.25,
                )
            ]
        ),
    )
    monkeypatch.setattr(hash_diff_pipeline_module, "ObjectStoreClient", FakeUploadObjectStoreClient)

    result = extract_validate_land_snapshot(
        source_dsn="postgresql://source",
        source_table="public.orders",
        contract=contract,
        object_store_config=ObjectStoreConfig(bucket="landing"),
        accepted_object_key="accepted/orders.ndjson.gz",
        error_object_key="accepted/errors.ndjson.gz",
        manifest_key="accepted/manifest.json",
        source_batch_size=1000,
    )

    assert result.accepted_object_key == "accepted/orders.ndjson.gz"
    assert result.valid_row_count == 2
    assert result.invalid_row_count == 0
    assert result.source_row_count == 2
    assert set(FakeUploadObjectStoreClient.uploads) == {
        "accepted/orders.ndjson.gz",
        "accepted/errors.ndjson.gz",
    }

    accepted_rows = _read_gzip_ndjson(FakeUploadObjectStoreClient.uploads["accepted/orders.ndjson.gz"])
    assert [row["id"] for row in accepted_rows] == [1, 2]
    assert all(isinstance(row["row_hash"], str) and row["row_hash"] for row in accepted_rows)

    assert _read_gzip_ndjson(FakeUploadObjectStoreClient.uploads["accepted/errors.ndjson.gz"]) == []
    assert FakeUploadObjectStoreClient.json_payloads["accepted/manifest.json"]["stage"] == "extract_validate_land"


def test_extract_validate_land_snapshot_does_not_publish_accepted_snapshot_on_validation_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    contract = _build_contract()
    FakeUploadObjectStoreClient.reset()

    monkeypatch.setattr(hash_diff_pipeline_module, "create_sqlalchemy_engine", lambda _: DummyEngine())
    monkeypatch.setattr(hash_diff_pipeline_module, "parse_table_name", lambda _: ("public", "orders"))
    monkeypatch.setattr(hash_diff_pipeline_module, "reflect_table", lambda *args, **kwargs: object())
    monkeypatch.setattr(hash_diff_pipeline_module, "_validate_source_columns", lambda *args, **kwargs: None)
    monkeypatch.setattr(
        hash_diff_pipeline_module,
        "_iter_source_row_batches",
        lambda **kwargs: iter([([{"id": None, "status": "NEW"}], 0.1)]),
    )
    monkeypatch.setattr(hash_diff_pipeline_module, "ObjectStoreClient", FakeUploadObjectStoreClient)

    with pytest.raises(ContractValidationError, match="invalid rows"):
        extract_validate_land_snapshot(
            source_dsn="postgresql://source",
            source_table="public.orders",
            contract=contract,
            object_store_config=ObjectStoreConfig(bucket="landing"),
            accepted_object_key="accepted/orders.ndjson.gz",
            error_object_key="accepted/errors.ndjson.gz",
            manifest_key="accepted/manifest.json",
            source_batch_size=1000,
        )

    assert "accepted/orders.ndjson.gz" not in FakeUploadObjectStoreClient.uploads
    assert "accepted/errors.ndjson.gz" in FakeUploadObjectStoreClient.uploads
    assert FakeUploadObjectStoreClient.json_payloads["accepted/manifest.json"]["accepted_object_key"] is None


def test_merge_accepted_snapshot_to_curated_loads_short_lived_staging_table(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    contract = _build_contract()
    engine = create_engine("sqlite://")
    FakeReaderObjectStoreClient.payloads = {
        "accepted/orders.ndjson.gz": gzip.compress(
            "\n".join(
                [
                    json.dumps({"id": 1, "status": "NEW", "row_hash": "hash-1"}),
                    json.dumps({"id": 2, "status": "PAID", "row_hash": "hash-2"}),
                ]
            ).encode("utf-8")
        )
    }
    captured: dict[str, object] = {}

    monkeypatch.setattr(hash_diff_pipeline_module, "create_sqlalchemy_engine", lambda _: engine)
    monkeypatch.setattr(hash_diff_pipeline_module, "ensure_schema", lambda *args, **kwargs: None)
    monkeypatch.setattr(
        hash_diff_pipeline_module,
        "parse_table_name",
        lambda table_name: ("main", table_name.split(".", 1)[-1]),
    )
    monkeypatch.setattr(hash_diff_pipeline_module, "ObjectStoreClient", FakeReaderObjectStoreClient)

    def _fake_run_hash_diff(**kwargs):
        source_table = str(kwargs["source_table"])
        table_name = source_table.split(".", 1)[1]
        with engine.connect() as connection:
            captured["row_count"] = connection.execute(
                text(f'SELECT COUNT(*) FROM "main"."{table_name}"')
            ).scalar_one()
            captured["row_hashes"] = connection.execute(
                text(f'SELECT row_hash FROM "main"."{table_name}" ORDER BY id')
            ).scalars().all()
        return hash_diff_pipeline_module.HashDiffResult(
            read_count=2,
            insert_count=1,
            update_count=1,
            delete_count=0,
            unchanged_count=0,
            processed_batches=1,
            source_read_seconds=0.0,
            diff_seconds=0.0,
            write_seconds=0.0,
            total_seconds=0.0,
        )

    monkeypatch.setattr(hash_diff_pipeline_module, "run_hash_diff", _fake_run_hash_diff)

    result = merge_accepted_snapshot_to_curated(
        target_dsn="sqlite://",
        target_table_curated="main.orders",
        contract=contract,
        object_store_config=ObjectStoreConfig(bucket="landing"),
        accepted_object_key="accepted/orders.ndjson.gz",
        merge_load_batch_size=1,
        source_batch_size=1000,
        upsert_batch_size=1000,
    )

    assert result.read_count == 2
    assert captured["row_count"] == 2
    assert captured["row_hashes"] == ["hash-1", "hash-2"]

    with engine.connect() as connection:
        remaining_staging_tables = connection.execute(
            text("SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name LIKE '_hd_merge_%'")
        ).scalar_one()
    assert remaining_staging_tables == 0
