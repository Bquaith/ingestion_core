from __future__ import annotations

from typing import Any, Mapping

from ingestion_core.strategies.incremental_audit.admin import ensure_source_audit_capture, resolve_watermark_mode
from ingestion_core.strategies.incremental_audit.apply import apply_delta_to_curated
from ingestion_core.strategies.incremental_audit.extract import extract_validate_land_delta
from ingestion_core.strategies.incremental_audit.types import AuditWatermark


def checkpoint_watermark_from_payload(checkpoint_payload: Mapping[str, Any] | None) -> AuditWatermark | None:
    if not checkpoint_payload:
        return None

    watermark_payload = checkpoint_payload.get("last_applied_watermark") or checkpoint_payload.get("window_end")
    if not isinstance(watermark_payload, Mapping):
        return None
    return AuditWatermark.from_mapping(watermark_payload)


__all__ = [
    "apply_delta_to_curated",
    "checkpoint_watermark_from_payload",
    "ensure_source_audit_capture",
    "extract_validate_land_delta",
    "resolve_watermark_mode",
]
