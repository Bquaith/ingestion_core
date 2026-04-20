from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Mapping

from ingestion_core.contracts.runtime import parse_iso_datetime

WATERMARK_MODE_AUTO = "auto"
WATERMARK_MODE_COMMIT_TIMESTAMP = "commit_timestamp"
WATERMARK_MODE_RECORDED_AT = "recorded_at"

DELTA_OP_UPSERT = "UPSERT"
DELTA_OP_DELETE = "DELETE"


def _coerce_datetime(value: Any, field_name: str) -> datetime:
    if isinstance(value, datetime):
        return value
    if isinstance(value, str) and value.strip():
        return parse_iso_datetime(value)
    raise ValueError(f"{field_name} must be a datetime or non-empty ISO datetime string")


@dataclass(frozen=True)
class AuditWatermark:
    ordering_ts: datetime
    event_id: int
    mode: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "ordering_ts": self.ordering_ts.isoformat(),
            "event_id": self.event_id,
            "mode": self.mode,
        }

    @classmethod
    def from_mapping(cls, payload: Mapping[str, Any]) -> "AuditWatermark":
        return cls(
            ordering_ts=_coerce_datetime(payload.get("ordering_ts"), "ordering_ts"),
            event_id=int(payload.get("event_id")),
            mode=str(payload.get("mode") or WATERMARK_MODE_RECORDED_AT),
        )


@dataclass(frozen=True)
class SourceAuditEvent:
    event_id: int
    op: str
    source_txid: int
    recorded_at: datetime
    ordering_ts: datetime
    key_json: dict[str, Any]
    row_before: dict[str, Any] | None
    row_after: dict[str, Any] | None
    changed_columns: list[str]

    @classmethod
    def from_mapping(cls, row: Mapping[str, Any]) -> "SourceAuditEvent":
        key_json = row.get("key_json") or {}
        row_before = row.get("row_before")
        row_after = row.get("row_after")
        changed_columns = row.get("changed_columns") or []

        if not isinstance(key_json, Mapping):
            raise ValueError("Audit event key_json must be an object")
        if row_before is not None and not isinstance(row_before, Mapping):
            raise ValueError("Audit event row_before must be an object or null")
        if row_after is not None and not isinstance(row_after, Mapping):
            raise ValueError("Audit event row_after must be an object or null")

        return cls(
            event_id=int(row["audit_event_id"]),
            op=str(row["op"]),
            source_txid=int(row["source_txid"]),
            recorded_at=_coerce_datetime(row["recorded_at"], "recorded_at"),
            ordering_ts=_coerce_datetime(row["ordering_ts"], "ordering_ts"),
            key_json=dict(key_json),
            row_before=dict(row_before) if row_before is not None else None,
            row_after=dict(row_after) if row_after is not None else None,
            changed_columns=[str(v) for v in changed_columns if str(v).strip()],
        )


@dataclass(frozen=True)
class NormalizedDeltaEvent:
    event_id: int
    event_ts: datetime
    op: str
    key: dict[str, Any]
    row: dict[str, Any] | None
    row_hash: str | None

    def to_payload(self) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "event_id": self.event_id,
            "event_ts": self.event_ts.isoformat(),
            "op": self.op,
            "key": dict(self.key),
        }
        if self.row is not None:
            payload["row"] = dict(self.row)
        if self.row_hash is not None:
            payload["row_hash"] = self.row_hash
        return payload

    @classmethod
    def from_payload(cls, payload: Mapping[str, Any]) -> "NormalizedDeltaEvent":
        key = payload.get("key") or {}
        row = payload.get("row")
        if not isinstance(key, Mapping):
            raise ValueError("Delta payload key must be an object")
        if row is not None and not isinstance(row, Mapping):
            raise ValueError("Delta payload row must be an object or null")

        row_hash = payload.get("row_hash")
        if row_hash is not None:
            row_hash = str(row_hash)

        return cls(
            event_id=int(payload["event_id"]),
            event_ts=_coerce_datetime(payload["event_ts"], "event_ts"),
            op=str(payload["op"]),
            key=dict(key),
            row=dict(row) if row is not None else None,
            row_hash=row_hash,
        )


@dataclass(frozen=True)
class AuditSetupResult:
    watermark_mode: str
    audit_schema_created: bool
    audit_table_created: bool
    trigger_function_created: bool
    trigger_created: bool
    index_count_created: int

    def to_dict(self) -> dict[str, Any]:
        return {
            "watermark_mode": self.watermark_mode,
            "audit_schema_created": self.audit_schema_created,
            "audit_table_created": self.audit_table_created,
            "trigger_function_created": self.trigger_function_created,
            "trigger_created": self.trigger_created,
            "index_count_created": self.index_count_created,
        }


@dataclass(frozen=True)
class ExtractValidateDeltaResult:
    delta_object_key: str | None
    error_object_key: str
    manifest_key: str
    source_event_count: int
    normalized_event_count: int
    upsert_event_count: int
    delete_event_count: int
    invalid_event_count: int
    processed_batches: int
    source_read_seconds: float
    window_start: AuditWatermark | None
    window_end: AuditWatermark | None
    watermark_mode: str

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
            "processed_batches": self.processed_batches,
            "source_read_seconds": self.source_read_seconds,
            "window_start": self.window_start.to_dict() if self.window_start else None,
            "window_end": self.window_end.to_dict() if self.window_end else None,
            "watermark_mode": self.watermark_mode,
        }


@dataclass(frozen=True)
class ApplyDeltaResult:
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
    last_applied_watermark: AuditWatermark | None

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
            "last_applied_watermark": self.last_applied_watermark.to_dict() if self.last_applied_watermark else None,
        }
