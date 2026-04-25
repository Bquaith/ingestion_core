from __future__ import annotations

import gzip
import io
import json
from datetime import datetime, timezone

import pytest
from sqlalchemy import create_engine, text

from ingestion_core.adapters.object_store import ObjectStoreConfig
from ingestion_core.contracts.types import ContractDefinition
import ingestion_core.strategies.incremental_audit.admin as incremental_admin_module
import ingestion_core.strategies.incremental_audit.extract as incremental_extract_module
import ingestion_core.strategies.common.delta_apply as delta_apply_module
from ingestion_core.strategies.incremental_audit import (
    AuditWatermark,
    SourceAuditEvent,
    apply_delta_to_curated,
    ensure_source_audit_capture,
    extract_validate_land_delta,
    resolve_watermark_mode,
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


class _ScalarResult:
    def __init__(self, value):
        self._value = value

    def scalar_one(self):
        return self._value


class _FakeConnection:
    def __init__(self) -> None:
        self.statements: list[str] = []

    def execute(self, statement, params=None):
        del params
        sql = str(statement)
        self.statements.append(sql)

        if "current_setting('track_commit_timestamp'" in sql:
            return _ScalarResult("on")
        if "FROM information_schema.schemata" in sql:
            return _ScalarResult(False)
        if "FROM pg_proc" in sql:
            return _ScalarResult(False)
        if "FROM pg_trigger" in sql:
            return _ScalarResult(False)
        if "FROM pg_indexes" in sql:
            return _ScalarResult(False)
        return _ScalarResult(None)


class _FakeBegin:
    def __init__(self, connection: _FakeConnection) -> None:
        self.connection = connection

    def __enter__(self) -> _FakeConnection:
        return self.connection

    def __exit__(self, exc_type, exc, tb) -> bool:
        return False


class _FakeConnect(_FakeBegin):
    pass


class FakeAdminEngine:
    def __init__(self) -> None:
        self.connection = _FakeConnection()

    def begin(self) -> _FakeBegin:
        return _FakeBegin(self.connection)

    def connect(self) -> _FakeConnect:
        return _FakeConnect(self.connection)

    def dispose(self) -> None:
        return None


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


def test_resolve_watermark_mode_prefers_commit_timestamp(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(incremental_admin_module, "create_sqlalchemy_engine", lambda _: FakeAdminEngine())

    assert resolve_watermark_mode("postgresql://source", "auto") == "commit_timestamp"


def test_ensure_source_audit_capture_creates_schema_table_trigger_and_indexes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fake_engine = FakeAdminEngine()
    monkeypatch.setattr(incremental_admin_module, "create_sqlalchemy_engine", lambda _: fake_engine)
    monkeypatch.setattr(incremental_admin_module, "table_exists", lambda *args, **kwargs: False)

    result = ensure_source_audit_capture(
        source_admin_dsn="postgresql://source",
        source_table="public.orders",
        source_audit_table="ingestion_meta.orders_audit",
        contract=_build_contract(),
        watermark_mode="auto",
        replace_existing_trigger=False,
    )

    assert result.watermark_mode == "commit_timestamp"
    assert result.audit_schema_created is True
    assert result.audit_table_created is True
    assert result.trigger_function_created is True
    assert result.trigger_created is True
    assert result.index_count_created == 2
    assert any("CREATE TABLE IF NOT EXISTS" in statement for statement in fake_engine.connection.statements)
    assert any("CREATE TRIGGER" in statement for statement in fake_engine.connection.statements)


def test_extract_validate_land_delta_splits_key_change_update_into_delete_and_upsert(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    contract = _build_contract()
    FakeUploadObjectStoreClient.reset()
    event_ts = datetime(2026, 4, 19, 10, 0, 0, tzinfo=timezone.utc)

    monkeypatch.setattr(incremental_extract_module, "create_sqlalchemy_engine", lambda _: DummyEngine())
    monkeypatch.setattr(incremental_extract_module, "parse_table_name", lambda _: ("ingestion_meta", "orders_audit"))
    monkeypatch.setattr(incremental_extract_module, "resolve_watermark_mode", lambda *args, **kwargs: "recorded_at")
    monkeypatch.setattr(
        incremental_extract_module,
        "_select_latest_watermark",
        lambda **kwargs: AuditWatermark(ordering_ts=event_ts, event_id=20, mode="recorded_at"),
    )
    monkeypatch.setattr(
        incremental_extract_module,
        "_iter_source_audit_event_batches",
        lambda **kwargs: iter(
            [
                (
                    [
                        SourceAuditEvent(
                            event_id=20,
                            op="U",
                            source_txid=10,
                            recorded_at=event_ts,
                            ordering_ts=event_ts,
                            key_json={"id": 1},
                            row_before={"id": 1, "status": "NEW"},
                            row_after={"id": 2, "status": "PAID"},
                            changed_columns=["id", "status"],
                        )
                    ],
                    0.15,
                )
            ]
        ),
    )
    monkeypatch.setattr(incremental_extract_module, "ObjectStoreClient", FakeUploadObjectStoreClient)

    result = extract_validate_land_delta(
        source_dsn="postgresql://source",
        source_audit_table="ingestion_meta.orders_audit",
        contract=contract,
        object_store_config=ObjectStoreConfig(bucket="landing"),
        delta_object_key="delta/orders.ndjson.gz",
        error_object_key="delta/errors.ndjson.gz",
        manifest_key="delta/manifest.json",
        extract_batch_size=1000,
    )

    assert result.delta_object_key == "delta/orders.ndjson.gz"
    assert result.source_event_count == 1
    assert result.normalized_event_count == 2
    assert result.upsert_event_count == 1
    assert result.delete_event_count == 1

    delta_rows = _read_gzip_ndjson(FakeUploadObjectStoreClient.uploads["delta/orders.ndjson.gz"])
    assert [row["op"] for row in delta_rows] == ["DELETE", "UPSERT"]
    assert delta_rows[0]["key"] == {"id": 1}
    assert delta_rows[1]["key"] == {"id": 2}
    assert delta_rows[1]["row"]["status"] == "PAID"
    assert isinstance(delta_rows[1]["row_hash"], str)
    assert FakeUploadObjectStoreClient.json_payloads["delta/manifest.json"]["window_end"]["event_id"] == 20


def test_extract_validate_land_delta_quarantines_invalid_events_and_still_publishes_delta(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    contract = _build_contract()
    FakeUploadObjectStoreClient.reset()
    event_ts = datetime(2026, 4, 19, 10, 0, 0, tzinfo=timezone.utc)

    monkeypatch.setattr(incremental_extract_module, "create_sqlalchemy_engine", lambda _: DummyEngine())
    monkeypatch.setattr(incremental_extract_module, "parse_table_name", lambda _: ("ingestion_meta", "orders_audit"))
    monkeypatch.setattr(incremental_extract_module, "resolve_watermark_mode", lambda *args, **kwargs: "recorded_at")
    monkeypatch.setattr(
        incremental_extract_module,
        "_select_latest_watermark",
        lambda **kwargs: AuditWatermark(ordering_ts=event_ts, event_id=30, mode="recorded_at"),
    )
    monkeypatch.setattr(
        incremental_extract_module,
        "_iter_source_audit_event_batches",
        lambda **kwargs: iter(
            [
                (
                    [
                        SourceAuditEvent(
                            event_id=30,
                            op="I",
                            source_txid=11,
                            recorded_at=event_ts,
                            ordering_ts=event_ts,
                            key_json={"id": None},
                            row_before=None,
                            row_after={"id": None, "status": "BROKEN"},
                            changed_columns=[],
                        ),
                        SourceAuditEvent(
                            event_id=31,
                            op="I",
                            source_txid=12,
                            recorded_at=event_ts,
                            ordering_ts=event_ts,
                            key_json={"id": 10},
                            row_before=None,
                            row_after={"id": 10, "status": "VALID"},
                            changed_columns=[],
                        )
                    ],
                    0.05,
                )
            ]
        ),
    )
    monkeypatch.setattr(incremental_extract_module, "ObjectStoreClient", FakeUploadObjectStoreClient)

    result = extract_validate_land_delta(
        source_dsn="postgresql://source",
        source_audit_table="ingestion_meta.orders_audit",
        contract=contract,
        object_store_config=ObjectStoreConfig(bucket="landing"),
        delta_object_key="delta/orders.ndjson.gz",
        error_object_key="delta/errors.ndjson.gz",
        manifest_key="delta/manifest.json",
        extract_batch_size=1000,
    )

    assert result.delta_object_key == "delta/orders.ndjson.gz"
    assert result.source_event_count == 2
    assert result.normalized_event_count == 1
    assert result.invalid_event_count == 1
    assert result.upsert_event_count == 1
    assert result.delete_event_count == 0

    delta_rows = _read_gzip_ndjson(FakeUploadObjectStoreClient.uploads["delta/orders.ndjson.gz"])
    assert len(delta_rows) == 1
    assert delta_rows[0]["key"] == {"id": 10}
    assert delta_rows[0]["row"]["status"] == "VALID"
    assert "delta/errors.ndjson.gz" in FakeUploadObjectStoreClient.uploads
    assert FakeUploadObjectStoreClient.json_payloads["delta/manifest.json"]["delta_object_key"] == "delta/orders.ndjson.gz"
    assert FakeUploadObjectStoreClient.json_payloads["delta/manifest.json"]["invalid_event_count"] == 1


def test_extract_validate_land_delta_publishes_empty_delta_when_all_events_are_invalid(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    contract = _build_contract()
    FakeUploadObjectStoreClient.reset()
    event_ts = datetime(2026, 4, 19, 10, 0, 0, tzinfo=timezone.utc)

    monkeypatch.setattr(incremental_extract_module, "create_sqlalchemy_engine", lambda _: DummyEngine())
    monkeypatch.setattr(incremental_extract_module, "parse_table_name", lambda _: ("ingestion_meta", "orders_audit"))
    monkeypatch.setattr(incremental_extract_module, "resolve_watermark_mode", lambda *args, **kwargs: "recorded_at")
    monkeypatch.setattr(
        incremental_extract_module,
        "_select_latest_watermark",
        lambda **kwargs: AuditWatermark(ordering_ts=event_ts, event_id=40, mode="recorded_at"),
    )
    monkeypatch.setattr(
        incremental_extract_module,
        "_iter_source_audit_event_batches",
        lambda **kwargs: iter(
            [
                (
                    [
                        SourceAuditEvent(
                            event_id=40,
                            op="I",
                            source_txid=21,
                            recorded_at=event_ts,
                            ordering_ts=event_ts,
                            key_json={"id": None},
                            row_before=None,
                            row_after={"id": None, "status": "BROKEN"},
                            changed_columns=[],
                        )
                    ],
                    0.05,
                )
            ]
        ),
    )
    monkeypatch.setattr(incremental_extract_module, "ObjectStoreClient", FakeUploadObjectStoreClient)

    result = extract_validate_land_delta(
        source_dsn="postgresql://source",
        source_audit_table="ingestion_meta.orders_audit",
        contract=contract,
        object_store_config=ObjectStoreConfig(bucket="landing"),
        delta_object_key="delta/orders.ndjson.gz",
        error_object_key="delta/errors.ndjson.gz",
        manifest_key="delta/manifest.json",
        extract_batch_size=1000,
    )

    assert result.delta_object_key == "delta/orders.ndjson.gz"
    assert result.source_event_count == 1
    assert result.normalized_event_count == 0
    assert result.invalid_event_count == 1

    delta_rows = _read_gzip_ndjson(FakeUploadObjectStoreClient.uploads["delta/orders.ndjson.gz"])
    assert delta_rows == []


def test_apply_delta_to_curated_loads_short_lived_staging_table_and_collapses_latest_events(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    contract = _build_contract()
    engine = create_engine("sqlite://")
    FakeReaderObjectStoreClient.payloads = {
        "delta/orders.ndjson.gz": gzip.compress(
            "\n".join(
                [
                    json.dumps(
                        {
                            "event_id": 1,
                            "event_ts": "2026-04-19T10:00:00+00:00",
                            "op": "UPSERT",
                            "key": {"id": 1},
                            "row": {"id": 1, "status": "NEW"},
                            "row_hash": "hash-old",
                        }
                    ),
                    json.dumps(
                        {
                            "event_id": 2,
                            "event_ts": "2026-04-19T10:00:01+00:00",
                            "op": "DELETE",
                            "key": {"id": 2},
                        }
                    ),
                    json.dumps(
                        {
                            "event_id": 3,
                            "event_ts": "2026-04-19T10:00:02+00:00",
                            "op": "UPSERT",
                            "key": {"id": 1},
                            "row": {"id": 1, "status": "PAID"},
                            "row_hash": "hash-new",
                        }
                    ),
                    json.dumps(
                        {
                            "event_id": 4,
                            "event_ts": "2026-04-19T10:00:03+00:00",
                            "op": "UPSERT",
                            "key": {"id": 3},
                            "row": {"id": 3, "status": "SAME"},
                            "row_hash": "hash-3",
                        }
                    ),
                ]
            ).encode("utf-8")
        )
    }
    captured: dict[str, object] = {}

    monkeypatch.setattr(delta_apply_module, "create_sqlalchemy_engine", lambda _: engine)
    monkeypatch.setattr(delta_apply_module, "ensure_schema", lambda *args, **kwargs: None)
    monkeypatch.setattr(
        delta_apply_module,
        "parse_table_name",
        lambda table_name: ("main", table_name.split(".", 1)[-1]),
    )
    monkeypatch.setattr(delta_apply_module, "ObjectStoreClient", FakeReaderObjectStoreClient)
    monkeypatch.setattr(delta_apply_module, "ensure_target_table_from_contract", lambda **kwargs: object())
    monkeypatch.setattr(delta_apply_module, "ensure_hash_state_table", lambda **kwargs: object())
    monkeypatch.setattr(delta_apply_module, "read_existing_hashes_for_keys", lambda **kwargs: {(1,): "hash-old", (3,): "hash-3"})
    monkeypatch.setattr(
        delta_apply_module,
        "upsert_changed_rows",
        lambda **kwargs: captured.setdefault("upsert_rows", list(kwargs["rows"])),
    )

    def _capture_delete_rows(**kwargs):
        captured["delete_keys"] = list(kwargs["key_tuples"])
        return len(kwargs["key_tuples"])

    monkeypatch.setattr(delta_apply_module, "delete_rows_by_keys", _capture_delete_rows)

    result = apply_delta_to_curated(
        target_dsn="sqlite://",
        target_table_curated="main.orders",
        contract=contract,
        object_store_config=ObjectStoreConfig(bucket="landing"),
        delta_object_key="delta/orders.ndjson.gz",
        load_batch_size=2,
        upsert_batch_size=1000,
    )

    assert result.read_count == 4
    assert result.effective_row_count == 3
    assert result.insert_count == 0
    assert result.update_count == 1
    assert result.delete_count == 1
    assert result.unchanged_count == 1
    assert captured["upsert_rows"] == [{"id": 1, "status": "PAID", "row_hash": "hash-new"}]
    assert captured["delete_keys"] == [(2,)]

    with engine.connect() as connection:
        remaining_staging_tables = connection.execute(
            text("SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name LIKE '_ia_delta_%'")
        ).scalar_one()
    assert remaining_staging_tables == 0
