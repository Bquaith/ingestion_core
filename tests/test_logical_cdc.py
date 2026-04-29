from __future__ import annotations

from datetime import datetime, timezone
import gzip
import io
import json
import struct

import pytest
from sqlalchemy.exc import SQLAlchemyError

from ingestion_core.adapters.object_store import ObjectStoreConfig
from ingestion_core.contracts.types import ContractDefinition
import ingestion_core.strategies.logical_cdc.admin as logical_admin_module
import ingestion_core.strategies.logical_cdc.extract as logical_extract_module
from ingestion_core.strategies.logical_cdc import (
    PgOutputDecodeError,
    PgOutputDecoder,
    ensure_source_logical_cdc_capture,
    int_to_lsn,
    lsn_to_int,
    max_lsn,
    resolve_checkpoint_lsn,
)
from ingestion_core.strategies.logical_cdc.types import (
    LogicalCdcSourceEvent,
    PgOutputRelation,
    PgOutputRelationColumn,
)


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


class _ScalarResult:
    def __init__(self, value):
        self._value = value

    def scalar_one(self):
        return self._value


class _MappingsResult:
    def __init__(self, rows: list[dict[str, object]]) -> None:
        self.rows = rows

    def one(self) -> dict[str, object]:
        return self.rows[0]

    def first(self) -> dict[str, object] | None:
        return self.rows[0] if self.rows else None


class _ResultWithMappings:
    def __init__(self, rows: list[dict[str, object]]) -> None:
        self._rows = rows

    def mappings(self) -> _MappingsResult:
        return _MappingsResult(self._rows)


class _FakeLogicalAdminConnection:
    def __init__(
        self,
        engine: "_FakeLogicalAdminEngine",
        autocommit: bool = False,
    ) -> None:
        self.engine = engine
        self.autocommit = autocommit
        self.write_performed = False

    def execution_options(self, **kwargs):
        return _FakeLogicalAdminConnection(
            self.engine,
            autocommit=kwargs.get("isolation_level") == "AUTOCOMMIT",
        )

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:
        return False

    def execute(self, statement, params=None):
        del params
        sql = str(statement)
        self.engine.statements.append(sql)
        self.engine.statement_records.append((sql, self.autocommit))
        if "current_setting('wal_level')" in sql:
            return _ResultWithMappings(
                [
                    {
                        "wal_level": self.engine.wal_level,
                        "max_replication_slots": self.engine.max_replication_slots,
                        "max_wal_senders": self.engine.max_wal_senders,
                    }
                ]
            )
        if sql.startswith("ALTER SYSTEM"):
            if self.engine.fail_alter_system:
                raise SQLAlchemyError("permission denied to execute ALTER SYSTEM")
            return _ScalarResult(None)
        if "pg_reload_conf" in sql:
            return _ScalarResult(True)
        if "FROM pg_publication WHERE pubname" in sql:
            return _ScalarResult(self.engine.publication_exists)
        if "FROM pg_publication p" in sql:
            return _ScalarResult(self.engine.table_in_publication)
        if sql.startswith("DROP PUBLICATION"):
            self.write_performed = True
            self.engine.publication_exists = False
            return _ScalarResult(None)
        if sql.startswith("CREATE PUBLICATION"):
            self.write_performed = True
            self.engine.publication_exists = True
            self.engine.table_in_publication = True
            return _ScalarResult(None)
        if sql.startswith("ALTER PUBLICATION"):
            self.write_performed = True
            self.engine.table_in_publication = True
            return _ScalarResult(None)
        if sql.startswith("ALTER TABLE"):
            self.write_performed = True
            return _ScalarResult(None)
        if "FROM pg_replication_slots" in sql:
            if self.engine.slot_plugin is None:
                return _ResultWithMappings([])
            return _ResultWithMappings(
                [
                    {
                        "slot_name": "slot_ingestion_sales_orders",
                        "plugin": self.engine.slot_plugin,
                        "slot_type": "logical",
                        "active": False,
                        "restart_lsn": None,
                        "confirmed_flush_lsn": None,
                    }
                ]
            )
        if "pg_create_logical_replication_slot" in sql:
            if self.write_performed and not self.autocommit:
                raise SQLAlchemyError("cannot create logical replication slot in transaction that has performed writes")
            self.engine.slot_plugin = "pgoutput"
            return _ResultWithMappings([{"slot_name": "slot_ingestion_sales_orders", "lsn": "0/16B6C80"}])
        return _ScalarResult(None)


class _FakeLogicalAdminEngine:
    def __init__(
        self,
        wal_level: str = "logical",
        max_replication_slots: int = 10,
        max_wal_senders: int = 10,
        fail_alter_system: bool = False,
        publication_exists: bool = False,
        table_in_publication: bool = False,
        slot_plugin: str | None = None,
    ) -> None:
        self.wal_level = wal_level
        self.max_replication_slots = max_replication_slots
        self.max_wal_senders = max_wal_senders
        self.fail_alter_system = fail_alter_system
        self.publication_exists = publication_exists
        self.table_in_publication = table_in_publication
        self.slot_plugin = slot_plugin
        self.statements: list[str] = []
        self.statement_records: list[tuple[str, bool]] = []

    def connect(self) -> _FakeLogicalAdminConnection:
        return _FakeLogicalAdminConnection(self)

    def begin(self) -> _FakeLogicalAdminConnection:
        return _FakeLogicalAdminConnection(self)

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


def _cstring(value: str) -> bytes:
    return value.encode("utf-8") + b"\x00"


def _u8(value: int | str) -> bytes:
    if isinstance(value, str):
        value = ord(value)
    return struct.pack("!B", value)


def _u16(value: int) -> bytes:
    return struct.pack("!H", value)


def _u32(value: int) -> bytes:
    return struct.pack("!I", value)


def _i32(value: int) -> bytes:
    return struct.pack("!i", value)


def _u64(value: int) -> bytes:
    return struct.pack("!Q", value)


def _i64(value: int) -> bytes:
    return struct.pack("!q", value)


def _tuple_data(values: list[str | None]) -> bytes:
    payload = _u16(len(values))
    for value in values:
        if value is None:
            payload += _u8("n")
        else:
            raw = value.encode("utf-8")
            payload += _u8("t") + _u32(len(raw)) + raw
    return payload


def _relation_message(relation_id: int = 10) -> bytes:
    return (
        _u8("R")
        + _u32(relation_id)
        + _cstring("public")
        + _cstring("orders")
        + _u8("d")
        + _u16(2)
        + _u8(1)
        + _cstring("id")
        + _u32(23)
        + _i32(-1)
        + _u8(0)
        + _cstring("status")
        + _u32(25)
        + _i32(-1)
    )


def _begin_message(xid: int = 42) -> bytes:
    return _u8("B") + _u64(lsn_to_int("0/16B6C50")) + _i64(0) + _u32(xid)


def _commit_message(commit_lsn: str = "0/16B6C80", end_lsn: str = "0/16B6CC0") -> bytes:
    return _u8("C") + _u8(0) + _u64(lsn_to_int(commit_lsn)) + _u64(lsn_to_int(end_lsn)) + _i64(0)


def test_lsn_helpers_round_trip_and_compare() -> None:
    assert lsn_to_int("0/16B6C80") == 23817344
    assert int_to_lsn(lsn_to_int("1/0")) == "1/0"
    assert max_lsn(None, "0/16B6C80", "0/16B6CC0") == "0/16B6CC0"


def test_resolve_checkpoint_lsn_does_not_advance_to_window_end_without_replication_progress() -> None:
    assert (
        resolve_checkpoint_lsn(
            start_lsn="0/16B6C80",
            delta_result={
                "processed_messages": 0,
                "last_decoded_lsn": None,
                "window_end_lsn": "0/16B6D00",
            },
            apply_result={"last_applied_lsn": None},
        )
        == "0/16B6C80"
    )


def test_resolve_checkpoint_lsn_prefers_applied_then_decoded_lsn() -> None:
    assert (
        resolve_checkpoint_lsn(
            start_lsn="0/16B6C80",
            delta_result={"last_decoded_lsn": "0/16B6CC0", "window_end_lsn": "0/16B6D00"},
            apply_result={"last_applied_lsn": "0/16B6CE0"},
        )
        == "0/16B6CE0"
    )
    assert (
        resolve_checkpoint_lsn(
            start_lsn="0/16B6C80",
            delta_result={"last_decoded_lsn": "0/16B6CC0", "window_end_lsn": "0/16B6D00"},
            apply_result={"last_applied_lsn": None},
        )
        == "0/16B6CC0"
    )


def test_pgoutput_decoder_emits_insert_update_delete_after_commit() -> None:
    decoder = PgOutputDecoder(source_table="public.orders")

    assert decoder.decode_message(_relation_message()) == []
    assert decoder.decode_message(_begin_message(xid=7)) == []
    assert decoder.decode_message(_u8("I") + _u32(10) + _u8("N") + _tuple_data(["1", "NEW"])) == []
    assert decoder.decode_message(
        _u8("U")
        + _u32(10)
        + _u8("K")
        + _tuple_data(["1", "NEW"])
        + _u8("N")
        + _tuple_data(["1", "PAID"])
    ) == []
    assert decoder.decode_message(_u8("D") + _u32(10) + _u8("K") + _tuple_data(["1", "PAID"])) == []

    events = decoder.decode_message(_commit_message())

    assert [event.source_op for event in events] == ["I", "U", "D"]
    assert [event.change_index for event in events] == [0, 1, 2]
    assert {event.commit_lsn for event in events} == {"0/16B6C80"}
    assert {event.end_lsn for event in events} == {"0/16B6CC0"}
    assert {event.xid for event in events} == {7}
    assert events[0].row_after == {"id": "1", "status": "NEW"}
    assert events[1].old_key == {"id": "1", "status": "NEW"}
    assert events[1].row_after == {"id": "1", "status": "PAID"}
    assert events[2].old_key == {"id": "1", "status": "PAID"}


def test_pgoutput_decoder_rejects_truncate() -> None:
    decoder = PgOutputDecoder(source_table="public.orders")

    with pytest.raises(PgOutputDecodeError, match="TRUNCATE"):
        decoder.decode_message(_u8("T"))


def test_extract_validate_land_wal_delta_splits_key_change_update_into_delete_and_upsert(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    contract = _build_contract()
    FakeUploadObjectStoreClient.reset()
    commit_ts = datetime(2026, 4, 22, 10, 0, 0, tzinfo=timezone.utc)
    relation = PgOutputRelation(
        relation_id=10,
        schema="public",
        table="orders",
        replica_identity="d",
        columns=[
            PgOutputRelationColumn(name="id", type_oid=23, atttypmod=-1, flags=1),
            PgOutputRelationColumn(name="status", type_oid=25, atttypmod=-1, flags=0),
        ],
    )
    source_event = LogicalCdcSourceEvent(
        source_op="U",
        commit_lsn="0/16B6C80",
        end_lsn="0/16B6CC0",
        change_index=5,
        xid=42,
        commit_ts=commit_ts,
        relation=relation,
        old_key={"id": "1", "status": "NEW"},
        row_after={"id": "2", "status": "PAID"},
    )

    monkeypatch.setattr(logical_extract_module, "_select_current_wal_lsn", lambda _: "0/16B6D00")
    monkeypatch.setattr(
        logical_extract_module,
        "_read_pgoutput_events",
        lambda **kwargs: ([source_event], 1, 0.05, "0/16B6CC0"),
    )
    monkeypatch.setattr(logical_extract_module, "ObjectStoreClient", FakeUploadObjectStoreClient)

    result = logical_extract_module.extract_validate_land_wal_delta(
        source_dsn="postgresql://source",
        source_replication_dsn="postgresql://source",
        source_table="public.orders",
        source_slot_name="slot_ingestion_sales_orders",
        source_publication_name="pub_ingestion_sales_orders",
        contract=contract,
        object_store_config=ObjectStoreConfig(bucket="landing"),
        delta_object_key="wal_delta/orders.ndjson.gz",
        error_object_key="wal_delta/errors.ndjson.gz",
        manifest_key="wal_delta/manifest.json",
        start_lsn="0/16B6C00",
        max_extract_seconds=1,
    )

    assert result.delta_object_key == "wal_delta/orders.ndjson.gz"
    assert result.source_event_count == 1
    assert result.normalized_event_count == 2
    assert result.upsert_event_count == 1
    assert result.delete_event_count == 1
    assert result.last_decoded_lsn == "0/16B6CC0"

    delta_rows = _read_gzip_ndjson(FakeUploadObjectStoreClient.uploads["wal_delta/orders.ndjson.gz"])
    assert [row["op"] for row in delta_rows] == ["DELETE", "UPSERT"]
    assert delta_rows[0]["key"] == {"id": 1}
    assert delta_rows[1]["key"] == {"id": 2}
    assert delta_rows[1]["row"]["status"] == "PAID"
    assert isinstance(delta_rows[1]["row_hash"], str)
    assert FakeUploadObjectStoreClient.json_payloads["wal_delta/manifest.json"]["window_end_lsn"] == "0/16B6D00"


def test_extract_validate_land_wal_delta_accepts_update_without_old_key(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    contract = _build_contract()
    FakeUploadObjectStoreClient.reset()
    commit_ts = datetime(2026, 4, 23, 10, 0, 0, tzinfo=timezone.utc)
    relation = PgOutputRelation(
        relation_id=10,
        schema="public",
        table="orders",
        replica_identity="d",
        columns=[
            PgOutputRelationColumn(name="id", type_oid=23, atttypmod=-1, flags=1),
            PgOutputRelationColumn(name="status", type_oid=25, atttypmod=-1, flags=0),
        ],
    )
    source_event = LogicalCdcSourceEvent(
        source_op="U",
        commit_lsn="0/16B6C80",
        end_lsn="0/16B6CC0",
        change_index=1,
        xid=43,
        commit_ts=commit_ts,
        relation=relation,
        old_key=None,
        row_after={"id": "1", "status": "PAID"},
    )

    monkeypatch.setattr(logical_extract_module, "_select_current_wal_lsn", lambda _: "0/16B6D00")
    monkeypatch.setattr(
        logical_extract_module,
        "_read_pgoutput_events",
        lambda **kwargs: ([source_event], 1, 0.05, "0/16B6CC0"),
    )
    monkeypatch.setattr(logical_extract_module, "ObjectStoreClient", FakeUploadObjectStoreClient)

    result = logical_extract_module.extract_validate_land_wal_delta(
        source_dsn="postgresql://source",
        source_replication_dsn="postgresql://source",
        source_table="public.orders",
        source_slot_name="slot_ingestion_sales_orders",
        source_publication_name="pub_ingestion_sales_orders",
        contract=contract,
        object_store_config=ObjectStoreConfig(bucket="landing"),
        delta_object_key="wal_delta/orders.ndjson.gz",
        error_object_key="wal_delta/errors.ndjson.gz",
        manifest_key="wal_delta/manifest.json",
        start_lsn="0/16B6C00",
        max_extract_seconds=1,
    )

    assert result.source_event_count == 1
    assert result.normalized_event_count == 1
    assert result.upsert_event_count == 1
    assert result.delete_event_count == 0

    delta_rows = _read_gzip_ndjson(FakeUploadObjectStoreClient.uploads["wal_delta/orders.ndjson.gz"])
    assert delta_rows[0]["op"] == "UPSERT"
    assert delta_rows[0]["key"] == {"id": 1}
    assert delta_rows[0]["row"]["status"] == "PAID"


def test_extract_validate_land_wal_delta_quarantines_entire_invalid_transaction_and_continues(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    contract = _build_contract()
    FakeUploadObjectStoreClient.reset()
    relation = PgOutputRelation(
        relation_id=10,
        schema="public",
        table="orders",
        replica_identity="d",
        columns=[
            PgOutputRelationColumn(name="id", type_oid=23, atttypmod=-1, flags=1),
            PgOutputRelationColumn(name="status", type_oid=25, atttypmod=-1, flags=0),
        ],
    )
    source_events = [
        LogicalCdcSourceEvent(
            source_op="I",
            commit_lsn="0/16B6C80",
            end_lsn="0/16B6CC0",
            change_index=0,
            xid=101,
            commit_ts=datetime(2026, 4, 24, 10, 0, 0, tzinfo=timezone.utc),
            relation=relation,
            old_key=None,
            row_after={"id": "1", "status": "NEW"},
        ),
        LogicalCdcSourceEvent(
            source_op="I",
            commit_lsn="0/16B6C80",
            end_lsn="0/16B6CC0",
            change_index=1,
            xid=101,
            commit_ts=datetime(2026, 4, 24, 10, 0, 0, tzinfo=timezone.utc),
            relation=relation,
            old_key=None,
            row_after={"status": "BROKEN"},
        ),
        LogicalCdcSourceEvent(
            source_op="I",
            commit_lsn="0/16B6D80",
            end_lsn="0/16B6DC0",
            change_index=0,
            xid=102,
            commit_ts=datetime(2026, 4, 24, 10, 1, 0, tzinfo=timezone.utc),
            relation=relation,
            old_key=None,
            row_after={"id": "2", "status": "PAID"},
        ),
    ]

    monkeypatch.setattr(logical_extract_module, "_select_current_wal_lsn", lambda _: "0/16B6E00")
    monkeypatch.setattr(
        logical_extract_module,
        "_read_pgoutput_events",
        lambda **kwargs: (source_events, 2, 0.05, "0/16B6DC0"),
    )
    monkeypatch.setattr(logical_extract_module, "ObjectStoreClient", FakeUploadObjectStoreClient)

    result = logical_extract_module.extract_validate_land_wal_delta(
        source_dsn="postgresql://source",
        source_replication_dsn="postgresql://source",
        source_table="public.orders",
        source_slot_name="slot_ingestion_sales_orders",
        source_publication_name="pub_ingestion_sales_orders",
        contract=contract,
        object_store_config=ObjectStoreConfig(bucket="landing"),
        delta_object_key="wal_delta/orders.ndjson.gz",
        error_object_key="wal_delta/errors.ndjson.gz",
        manifest_key="wal_delta/manifest.json",
        start_lsn="0/16B6C00",
        max_extract_seconds=1,
    )

    assert result.delta_object_key == "wal_delta/orders.ndjson.gz"
    assert result.source_event_count == 3
    assert result.normalized_event_count == 1
    assert result.upsert_event_count == 1
    assert result.delete_event_count == 0
    assert result.invalid_event_count == 1
    assert result.invalid_transaction_count == 1
    assert result.quarantined_event_count == 2
    assert result.quarantined_transaction_count == 1
    assert result.last_decoded_lsn == "0/16B6DC0"

    delta_rows = _read_gzip_ndjson(FakeUploadObjectStoreClient.uploads["wal_delta/orders.ndjson.gz"])
    assert len(delta_rows) == 1
    assert delta_rows[0]["commit_lsn"] == "0/16B6D80"
    assert delta_rows[0]["end_lsn"] == "0/16B6DC0"
    assert delta_rows[0]["change_index"] == 0
    assert delta_rows[0]["op"] == "UPSERT"
    assert delta_rows[0]["key"] == {"id": 2}
    assert delta_rows[0]["row"] == {"id": 2, "status": "PAID"}
    assert isinstance(delta_rows[0]["row_hash"], str)
    assert delta_rows[0]["xid"] == 102
    assert delta_rows[0]["commit_ts"] == "2026-04-24T10:01:00+00:00"

    error_rows = _read_gzip_ndjson(FakeUploadObjectStoreClient.uploads["wal_delta/errors.ndjson.gz"])
    assert len(error_rows) == 1
    assert error_rows[0]["field"] == "id"
    assert error_rows[0]["code"] == "missing_field"

    manifest = FakeUploadObjectStoreClient.json_payloads["wal_delta/manifest.json"]
    assert manifest["delta_object_key"] == "wal_delta/orders.ndjson.gz"
    assert manifest["invalid_transaction_count"] == 1
    assert manifest["quarantined_event_count"] == 2
    assert manifest["quarantined_transaction_count"] == 1


def test_extract_validate_land_wal_delta_uploads_empty_delta_when_all_transactions_are_quarantined(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    contract = _build_contract()
    FakeUploadObjectStoreClient.reset()
    relation = PgOutputRelation(
        relation_id=10,
        schema="public",
        table="orders",
        replica_identity="d",
        columns=[
            PgOutputRelationColumn(name="id", type_oid=23, atttypmod=-1, flags=1),
            PgOutputRelationColumn(name="status", type_oid=25, atttypmod=-1, flags=0),
        ],
    )
    source_event = LogicalCdcSourceEvent(
        source_op="I",
        commit_lsn="0/16B6C80",
        end_lsn="0/16B6CC0",
        change_index=0,
        xid=201,
        commit_ts=datetime(2026, 4, 24, 11, 0, 0, tzinfo=timezone.utc),
        relation=relation,
        old_key=None,
        row_after={"status": "BROKEN"},
    )

    monkeypatch.setattr(logical_extract_module, "_select_current_wal_lsn", lambda _: "0/16B6D00")
    monkeypatch.setattr(
        logical_extract_module,
        "_read_pgoutput_events",
        lambda **kwargs: ([source_event], 1, 0.05, "0/16B6CC0"),
    )
    monkeypatch.setattr(logical_extract_module, "ObjectStoreClient", FakeUploadObjectStoreClient)

    result = logical_extract_module.extract_validate_land_wal_delta(
        source_dsn="postgresql://source",
        source_replication_dsn="postgresql://source",
        source_table="public.orders",
        source_slot_name="slot_ingestion_sales_orders",
        source_publication_name="pub_ingestion_sales_orders",
        contract=contract,
        object_store_config=ObjectStoreConfig(bucket="landing"),
        delta_object_key="wal_delta/orders.ndjson.gz",
        error_object_key="wal_delta/errors.ndjson.gz",
        manifest_key="wal_delta/manifest.json",
        start_lsn="0/16B6C00",
        max_extract_seconds=1,
    )

    assert result.delta_object_key == "wal_delta/orders.ndjson.gz"
    assert result.source_event_count == 1
    assert result.normalized_event_count == 0
    assert result.invalid_event_count == 1
    assert result.invalid_transaction_count == 1
    assert result.quarantined_event_count == 1
    assert result.quarantined_transaction_count == 1

    assert _read_gzip_ndjson(FakeUploadObjectStoreClient.uploads["wal_delta/orders.ndjson.gz"]) == []
    error_rows = _read_gzip_ndjson(FakeUploadObjectStoreClient.uploads["wal_delta/errors.ndjson.gz"])
    assert len(error_rows) == 1
    assert error_rows[0]["field"] == "id"
    assert FakeUploadObjectStoreClient.json_payloads["wal_delta/manifest.json"]["delta_object_key"] == (
        "wal_delta/orders.ndjson.gz"
    )


def test_extract_validate_land_wal_delta_rejects_invalid_idle_timeout() -> None:
    with pytest.raises(ValueError, match="idle_timeout_seconds"):
        logical_extract_module.extract_validate_land_wal_delta(
            source_dsn="postgresql://source",
            source_replication_dsn="postgresql://source",
            source_table="public.orders",
            source_slot_name="slot_ingestion_sales_orders",
            source_publication_name="pub_ingestion_sales_orders",
            contract=_build_contract(),
            object_store_config=ObjectStoreConfig(bucket="landing"),
            delta_object_key="wal_delta/orders.ndjson.gz",
            error_object_key="wal_delta/errors.ndjson.gz",
            manifest_key="wal_delta/manifest.json",
            idle_timeout_seconds=0,
        )


def test_ensure_source_logical_cdc_capture_rejects_wal2json_without_connecting() -> None:
    with pytest.raises(ValueError, match="wal2json"):
        ensure_source_logical_cdc_capture(
            source_admin_dsn="postgresql://source",
            source_table="public.orders",
            source_publication_name="pub_ingestion_sales_orders",
            source_slot_name="slot_ingestion_sales_orders",
            contract=_build_contract(),
            output_plugin="wal2json",
        )


def test_ensure_source_logical_cdc_capture_requires_manual_wal_settings_without_auto_configure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fake_engine = _FakeLogicalAdminEngine(wal_level="replica", max_replication_slots=0, max_wal_senders=0)
    monkeypatch.setattr(logical_admin_module, "create_sqlalchemy_engine", lambda _: fake_engine)

    with pytest.raises(ValueError, match="auto_configure_wal_settings=true"):
        ensure_source_logical_cdc_capture(
            source_admin_dsn="postgresql://source",
            source_table="public.orders",
            source_publication_name="pub_ingestion_sales_orders",
            source_slot_name="slot_ingestion_sales_orders",
            contract=_build_contract(),
        )

    assert not any(statement.startswith("ALTER SYSTEM") for statement in fake_engine.statements)


def test_ensure_source_logical_cdc_capture_writes_wal_settings_and_requires_restart(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fake_engine = _FakeLogicalAdminEngine(wal_level="replica", max_replication_slots=0, max_wal_senders=0)
    monkeypatch.setattr(logical_admin_module, "create_sqlalchemy_engine", lambda _: fake_engine)

    with pytest.raises(ValueError, match="require PostgreSQL restart"):
        ensure_source_logical_cdc_capture(
            source_admin_dsn="postgresql://source",
            source_table="public.orders",
            source_publication_name="pub_ingestion_sales_orders",
            source_slot_name="slot_ingestion_sales_orders",
            contract=_build_contract(),
            auto_configure_wal_settings=True,
            desired_max_replication_slots=12,
            desired_max_wal_senders=8,
        )

    assert "ALTER SYSTEM SET wal_level = 'logical'" in fake_engine.statements
    assert "ALTER SYSTEM SET max_replication_slots = '12'" in fake_engine.statements
    assert "ALTER SYSTEM SET max_wal_senders = '8'" in fake_engine.statements
    assert "SELECT pg_reload_conf()" in fake_engine.statements
    assert not any("CREATE PUBLICATION" in statement for statement in fake_engine.statements)


def test_ensure_source_logical_cdc_capture_reports_missing_alter_system_privileges(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fake_engine = _FakeLogicalAdminEngine(
        wal_level="replica",
        max_replication_slots=0,
        max_wal_senders=0,
        fail_alter_system=True,
    )
    monkeypatch.setattr(logical_admin_module, "create_sqlalchemy_engine", lambda _: fake_engine)

    with pytest.raises(ValueError, match="WAL auto-configuration failed"):
        ensure_source_logical_cdc_capture(
            source_admin_dsn="postgresql://source",
            source_table="public.orders",
            source_publication_name="pub_ingestion_sales_orders",
            source_slot_name="slot_ingestion_sales_orders",
            contract=_build_contract(),
            auto_configure_wal_settings=True,
        )


def test_ensure_source_logical_cdc_capture_creates_slot_after_setup_transaction_in_autocommit(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fake_engine = _FakeLogicalAdminEngine(
        wal_level="logical",
        max_replication_slots=10,
        max_wal_senders=10,
        publication_exists=False,
        slot_plugin=None,
    )
    monkeypatch.setattr(logical_admin_module, "create_sqlalchemy_engine", lambda _: fake_engine)

    result = ensure_source_logical_cdc_capture(
        source_admin_dsn="postgresql://source",
        source_table="public.orders",
        source_publication_name="pub_ingestion_sales_orders",
        source_slot_name="slot_ingestion_sales_orders",
        contract=_build_contract(),
        create_slot_if_missing=True,
    )

    assert result.publication_created is True
    assert result.table_added_to_publication is True
    assert result.slot_created is True
    create_publication_index = next(
        index for index, statement in enumerate(fake_engine.statements) if statement.startswith("CREATE PUBLICATION")
    )
    create_slot_index = next(
        index for index, statement in enumerate(fake_engine.statements) if "pg_create_logical_replication_slot" in statement
    )
    assert create_slot_index > create_publication_index
    assert any(
        "pg_create_logical_replication_slot" in statement and autocommit
        for statement, autocommit in fake_engine.statement_records
    )
