from __future__ import annotations

import hashlib
from datetime import date, datetime
from decimal import Decimal
from typing import Any, Mapping, Sequence

NULL_SENTINEL = "<NULL>"


def canonical_serialize(value: Any) -> str:
    if value is None:
        return NULL_SENTINEL
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, Decimal):
        return format(value, "f")
    if isinstance(value, (int, float)):
        return str(value)
    return str(value)


def serialize_row(row: Mapping[str, Any], ordered_fields: Sequence[str]) -> str:
    serialized = [canonical_serialize(row.get(field)) for field in ordered_fields]
    return "|".join(serialized)


def calculate_row_hash(row: Mapping[str, Any], ordered_fields: Sequence[str]) -> str:
    payload = serialize_row(row, ordered_fields)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()
