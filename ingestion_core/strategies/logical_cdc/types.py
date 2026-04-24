from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Mapping

from ingestion_core.contracts.runtime import parse_iso_datetime

OUTPUT_PLUGIN_PGOUTPUT = "pgoutput"

REPLICA_IDENTITY_DEFAULT = "default"
REPLICA_IDENTITY_FULL = "full"

CDC_OP_UPSERT = "UPSERT"
CDC_OP_DELETE = "DELETE"


def lsn_to_int(lsn: str) -> int:
    left, right = str(lsn).strip().split("/", 1)
    return (int(left, 16) << 32) + int(right, 16)


def int_to_lsn(value: int) -> str:
    left = value >> 32
    right = value & 0xFFFFFFFF
    return f"{left:X}/{right:X}"


def max_lsn(*values: str | None) -> str | None:
    present = [value for value in values if value]
    if not present:
        return None
    return max(present, key=lsn_to_int)


def _coerce_datetime(value: Any, field_name: str) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value
    if isinstance(value, str) and value.strip():
        return parse_iso_datetime(value)
    raise ValueError(f"{field_name} must be null, datetime or non-empty ISO datetime string")


@dataclass(frozen=True)
class LogicalCdcCheckpoint:
    last_applied_lsn: str | None
    last_flushed_lsn: str | None

    def to_dict(self) -> dict[str, str | None]:
        return {
            "last_applied_lsn": self.last_applied_lsn,
            "last_flushed_lsn": self.last_flushed_lsn,
        }

    @classmethod
    def from_mapping(cls, payload: Mapping[str, Any] | None) -> "LogicalCdcCheckpoint":
        if not payload:
            return cls(last_applied_lsn=None, last_flushed_lsn=None)
        return cls(
            last_applied_lsn=str(payload.get("last_applied_lsn") or "").strip() or None,
            last_flushed_lsn=str(payload.get("last_flushed_lsn") or "").strip() or None,
        )


@dataclass(frozen=True)
class LogicalCdcSetupResult:
    wal_level: str
    max_replication_slots: int
    max_wal_senders: int
    publication_created: bool
    table_added_to_publication: bool
    slot_created: bool
    replica_identity_changed: bool
    output_plugin: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "wal_level": self.wal_level,
            "max_replication_slots": self.max_replication_slots,
            "max_wal_senders": self.max_wal_senders,
            "publication_created": self.publication_created,
            "table_added_to_publication": self.table_added_to_publication,
            "slot_created": self.slot_created,
            "replica_identity_changed": self.replica_identity_changed,
            "output_plugin": self.output_plugin,
        }


@dataclass(frozen=True)
class PgOutputRelationColumn:
    name: str
    type_oid: int
    atttypmod: int
    flags: int


@dataclass(frozen=True)
class PgOutputRelation:
    relation_id: int
    schema: str
    table: str
    replica_identity: str
    columns: list[PgOutputRelationColumn]

    @property
    def qualified_name(self) -> str:
        return f"{self.schema}.{self.table}"

    @property
    def column_names(self) -> list[str]:
        return [column.name for column in self.columns]


@dataclass(frozen=True)
class LogicalCdcSourceEvent:
    source_op: str
    commit_lsn: str
    end_lsn: str
    change_index: int
    xid: int | None
    commit_ts: datetime | None
    relation: PgOutputRelation
    old_key: dict[str, Any] | None
    row_after: dict[str, Any] | None


@dataclass(frozen=True)
class LogicalCdcDeltaEvent:
    commit_lsn: str
    end_lsn: str
    change_index: int
    op: str
    key: dict[str, Any]
    row: dict[str, Any] | None
    row_hash: str | None
    xid: int | None = None
    commit_ts: datetime | None = None

    def to_payload(self) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "commit_lsn": self.commit_lsn,
            "end_lsn": self.end_lsn,
            "change_index": self.change_index,
            "op": self.op,
            "key": dict(self.key),
        }
        if self.row is not None:
            payload["row"] = dict(self.row)
        if self.row_hash is not None:
            payload["row_hash"] = self.row_hash
        if self.xid is not None:
            payload["xid"] = self.xid
        if self.commit_ts is not None:
            payload["commit_ts"] = self.commit_ts.isoformat()
        return payload

    @classmethod
    def from_payload(cls, payload: Mapping[str, Any]) -> "LogicalCdcDeltaEvent":
        key = payload.get("key") or {}
        row = payload.get("row")
        if not isinstance(key, Mapping):
            raise ValueError("Logical CDC delta payload key must be an object")
        if row is not None and not isinstance(row, Mapping):
            raise ValueError("Logical CDC delta payload row must be an object or null")
        row_hash = payload.get("row_hash")
        return cls(
            commit_lsn=str(payload["commit_lsn"]),
            end_lsn=str(payload.get("end_lsn") or payload["commit_lsn"]),
            change_index=int(payload["change_index"]),
            op=str(payload["op"]),
            key=dict(key),
            row=dict(row) if row is not None else None,
            row_hash=str(row_hash) if row_hash is not None else None,
            xid=int(payload["xid"]) if payload.get("xid") is not None else None,
            commit_ts=_coerce_datetime(payload.get("commit_ts"), "commit_ts"),
        )


@dataclass(frozen=True)
class ExtractValidateLogicalCdcResult:
    delta_object_key: str | None
    error_object_key: str
    manifest_key: str
    source_event_count: int
    normalized_event_count: int
    upsert_event_count: int
    delete_event_count: int
    invalid_event_count: int
    processed_messages: int
    source_read_seconds: float
    window_start_lsn: str | None
    window_end_lsn: str | None
    last_decoded_lsn: str | None
    output_plugin: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "delta_object_key": self.delta_object_key,
            "error_object_key": self.error_object_key,
            "manifest_key": self.manifest_key,
            "source_event_count": self.source_event_count,
            "normalized_event_count": self.normalized_event_count,
            "upsert_event_count": self.upsert_event_count,
            "delete_event_count": self.delete_event_count,
            "invalid_event_count": self.invalid_event_count,
            "processed_messages": self.processed_messages,
            "source_read_seconds": self.source_read_seconds,
            "window_start_lsn": self.window_start_lsn,
            "window_end_lsn": self.window_end_lsn,
            "last_decoded_lsn": self.last_decoded_lsn,
            "output_plugin": self.output_plugin,
        }


@dataclass(frozen=True)
class LogicalCdcApplyResult:
    read_count: int
    effective_row_count: int
    insert_count: int
    update_count: int
    delete_count: int
    unchanged_count: int
    processed_batches: int
    load_seconds: float
    diff_seconds: float
    write_seconds: float
    total_seconds: float
    last_applied_lsn: str | None

    def to_dict(self) -> dict[str, Any]:
        return {
            "read_count": self.read_count,
            "effective_row_count": self.effective_row_count,
            "insert_count": self.insert_count,
            "update_count": self.update_count,
            "delete_count": self.delete_count,
            "unchanged_count": self.unchanged_count,
            "processed_batches": self.processed_batches,
            "load_seconds": self.load_seconds,
            "diff_seconds": self.diff_seconds,
            "write_seconds": self.write_seconds,
            "total_seconds": self.total_seconds,
            "last_applied_lsn": self.last_applied_lsn,
        }
