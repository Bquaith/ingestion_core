from __future__ import annotations

import hashlib
from datetime import date, datetime
from decimal import Decimal

from ingestion_platform.hashing import NULL_SENTINEL, calculate_row_hash, canonical_serialize, serialize_row


def test_canonical_serialize_rules() -> None:
    assert canonical_serialize(None) == NULL_SENTINEL
    assert canonical_serialize(date(2025, 1, 1)) == "2025-01-01"
    assert canonical_serialize(datetime(2025, 1, 1, 12, 0, 5)) == "2025-01-01T12:00:05"
    assert canonical_serialize(Decimal("10.50")) == "10.50"
    assert canonical_serialize(42) == "42"


def test_serialize_row_fixed_order() -> None:
    row = {"amount": 10, "id": 1, "status": None}
    assert serialize_row(row, ["id", "amount", "status"]) == "1|10|<NULL>"


def test_calculate_row_hash_is_sha256() -> None:
    row = {"id": 1, "status": "NEW", "amount": "10.00"}
    payload = "1|NEW|10.00".encode("utf-8")
    expected = hashlib.sha256(payload).hexdigest()

    assert calculate_row_hash(row, ["id", "status", "amount"]) == expected
