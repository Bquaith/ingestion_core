from __future__ import annotations

from typing import Any, Mapping

from sqlalchemy import BigInteger, Text

from ingestion_core.adapters.object_store import ObjectStoreConfig
from ingestion_core.contracts.types import ContractDefinition
from ingestion_core.strategies.common import (
    DELTA_OP_DELETE,
    DELTA_OP_UPSERT,
    DeltaMetadataColumn,
    ParsedDeltaEvent,
    apply_delta_artifact_to_curated,
    quote_identifier,
)
from ingestion_core.strategies.logical_cdc.types import LogicalCdcApplyResult, LogicalCdcDeltaEvent, lsn_to_int


def _parse_logical_cdc_delta_event(payload: Mapping[str, Any]) -> ParsedDeltaEvent:
    event = LogicalCdcDeltaEvent.from_payload(payload)
    position = {
        "last_applied_lsn": event.end_lsn,
        "last_flushed_lsn": event.end_lsn,
        "commit_lsn": event.commit_lsn,
        "change_index": event.change_index,
    }
    return ParsedDeltaEvent(
        op=event.op,
        key=event.key,
        row=event.row,
        row_hash=event.row_hash,
        metadata={
            "commit_lsn": event.commit_lsn,
            "change_index": event.change_index,
        },
        position=position,
        position_sort_key=(lsn_to_int(event.commit_lsn), event.change_index),
    )


def apply_wal_delta_to_curated(
    target_dsn: str,
    target_table_curated: str,
    contract: ContractDefinition,
    object_store_config: ObjectStoreConfig,
    delta_object_key: str,
    load_batch_size: int = 1000,
    upsert_batch_size: int = 1000,
) -> LogicalCdcApplyResult:
    result = apply_delta_artifact_to_curated(
        target_dsn=target_dsn,
        target_table_curated=target_table_curated,
        contract=contract,
        object_store_config=object_store_config,
        delta_object_key=delta_object_key,
        parse_event_payload=_parse_logical_cdc_delta_event,
        metadata_columns=[
            DeltaMetadataColumn("commit_lsn", Text(), nullable=False),
            DeltaMetadataColumn("change_index", BigInteger(), nullable=False),
        ],
        order_by_desc_sql=[
            f"{quote_identifier('commit_lsn')}::pg_lsn DESC",
            f"{quote_identifier('change_index')} DESC",
        ],
        staging_table_prefix="lc_delta",
        load_batch_size=load_batch_size,
        upsert_batch_size=upsert_batch_size,
    )
    last_applied_lsn = None
    if result.last_position is not None:
        last_applied_lsn = str(result.last_position.get("last_applied_lsn") or "") or None
    return LogicalCdcApplyResult(
        read_count=result.read_count,
        effective_row_count=result.effective_row_count,
        insert_count=result.insert_count,
        update_count=result.update_count,
        delete_count=result.delete_count,
        unchanged_count=result.unchanged_count,
        processed_batches=result.processed_batches,
        load_seconds=result.load_seconds,
        diff_seconds=result.diff_seconds,
        write_seconds=result.write_seconds,
        total_seconds=result.total_seconds,
        last_applied_lsn=last_applied_lsn,
    )


__all__ = [
    "DELTA_OP_DELETE",
    "DELTA_OP_UPSERT",
    "apply_wal_delta_to_curated",
]
