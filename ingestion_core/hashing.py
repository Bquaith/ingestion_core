from __future__ import annotations

import base64
import hashlib
from datetime import date, datetime
from decimal import Decimal
import json
from typing import Any, Mapping, Sequence

NULL_SENTINEL = "<NULL>"


def _normalize_nested_value(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(key): _normalize_nested_value(item) for key, item in sorted(value.items(), key=lambda pair: str(pair[0]))}
    if isinstance(value, (list, tuple)):
        return [_normalize_nested_value(item) for item in value]
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, Decimal):
        return format(value, "f")
    if isinstance(value, (bytes, bytearray, memoryview)):
        return base64.b64encode(bytes(value)).decode("ascii")
    return value


def canonical_serialize(value: Any) -> str:
    if value is None:
        return NULL_SENTINEL
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, Decimal):
        return format(value, "f")
    if isinstance(value, (dict, list, tuple)):
        return json.dumps(_normalize_nested_value(value), ensure_ascii=True, sort_keys=True, separators=(",", ":"))
    if isinstance(value, (bytes, bytearray, memoryview)):
        return base64.b64encode(bytes(value)).decode("ascii")
    if isinstance(value, (int, float)):
        return str(value)
    return str(value)


def serialize_row(row: Mapping[str, Any], ordered_fields: Sequence[str]) -> str:
    serialized = [canonical_serialize(row.get(field)) for field in ordered_fields]
    return "|".join(serialized)


def calculate_row_hash(row: Mapping[str, Any], ordered_fields: Sequence[str]) -> str:
    payload = serialize_row(row, ordered_fields)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()
