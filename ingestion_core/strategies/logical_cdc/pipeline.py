from __future__ import annotations

from typing import Any, Mapping

from ingestion_core.strategies.logical_cdc.admin import ensure_source_logical_cdc_capture
from ingestion_core.strategies.logical_cdc.apply import apply_wal_delta_to_curated
from ingestion_core.strategies.logical_cdc.extract import extract_validate_land_wal_delta
from ingestion_core.strategies.logical_cdc.types import LogicalCdcCheckpoint, lsn_to_int


def checkpoint_lsn_from_payload(checkpoint_payload: Mapping[str, Any] | None) -> LogicalCdcCheckpoint:
    if not checkpoint_payload:
        return LogicalCdcCheckpoint.from_mapping(None)
    return LogicalCdcCheckpoint.from_mapping(checkpoint_payload)


def resolve_checkpoint_lsn(
    start_lsn: str | None,
    delta_result: Mapping[str, Any] | None,
    apply_result: Mapping[str, Any] | None,
) -> str | None:
    if apply_result:
        last_applied_lsn = str(apply_result.get("last_applied_lsn") or "").strip() or None
        if last_applied_lsn:
            return last_applied_lsn
    if delta_result:
        last_decoded_lsn = str(delta_result.get("last_decoded_lsn") or "").strip() or None
        if last_decoded_lsn:
            return last_decoded_lsn
    return start_lsn


def ack_logical_replication_slot(
    source_replication_dsn: str,
    source_slot_name: str,
    source_publication_name: str,
    flush_lsn: str,
) -> dict[str, str]:
    import psycopg2
    from psycopg2.extras import LogicalReplicationConnection

    conn = psycopg2.connect(source_replication_dsn, connection_factory=LogicalReplicationConnection)
    try:
        cur = conn.cursor()
        try:
            cur.start_replication(
                slot_name=source_slot_name,
                start_lsn=flush_lsn,
                options={
                    "proto_version": "1",
                    "publication_names": source_publication_name,
                    "messages": "false",
                },
                decode=False,
            )
            cur.send_feedback(flush_lsn=lsn_to_int(flush_lsn), reply=True)
        finally:
            cur.close()
    finally:
        conn.close()

    return {
        "source_slot_name": source_slot_name,
        "flushed_lsn": flush_lsn,
    }


__all__ = [
    "ack_logical_replication_slot",
    "apply_wal_delta_to_curated",
    "checkpoint_lsn_from_payload",
    "ensure_source_logical_cdc_capture",
    "extract_validate_land_wal_delta",
    "resolve_checkpoint_lsn",
]
