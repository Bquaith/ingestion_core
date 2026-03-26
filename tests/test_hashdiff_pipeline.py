from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal

import pytest
from sqlalchemy import Date, DateTime, Integer, Text
from sqlalchemy.dialects.postgresql import JSONB, UUID as PGUUID

from ingestion_core.hash_diff import ContractDefinition
from ingestion_core.hash_diff_pipeline import (
    _coerce_contract_value,
    _sqlalchemy_type_from_contract_field,
    _summarize_validation_errors,
)
from ingestion_core.object_store import ObjectStoreConfig


def test_coerce_contract_value_parses_decimal_and_timestamp() -> None:
    coerced_decimal = _coerce_contract_value("10.50", "decimal")
    coerced_timestamp = _coerce_contract_value("2025-01-01T10:00:00Z", "timestamp")

    assert coerced_decimal == Decimal("10.50")
    assert coerced_timestamp == datetime(2025, 1, 1, 10, 0, 0, tzinfo=timezone.utc)


def test_coerce_contract_value_rejects_invalid_boolean() -> None:
    with pytest.raises(ValueError, match="expected boolean value"):
        _coerce_contract_value("maybe", "boolean")


def test_object_store_normalize_key_is_idempotent_for_prefixed_keys() -> None:
    config = ObjectStoreConfig(bucket="landing", prefix="accepted")

    assert config.normalize_key("accepted/sales/orders/file.json") == "accepted/sales/orders/file.json"


def test_summarize_validation_errors_includes_row_field_code_and_message() -> None:
    summary = _summarize_validation_errors(
        [
            {
                "row_number": 7,
                "field": "gender",
                "code": "schema.enum",
                "message": "Value must be one of the contract enum values",
            }
        ]
    )

    assert summary == "row 7, field gender, code schema.enum: Value must be one of the contract enum values"


def test_sqlalchemy_type_from_contract_field_uses_schema_formats_and_json_containers() -> None:
    contract = ContractDefinition.from_registry_payload(
        {
            "contract_id": "c1",
            "target_layer": "raw",
            "version": "1",
            "checksum": "abc",
            "schema_json": {
                "type": "object",
                "properties": {
                    "id": {"type": "string", "format": "uuid"},
                    "created_at": {"type": "string", "format": "date-time"},
                    "birthday": {"type": "string", "format": "date"},
                    "amount": {"type": "integer"},
                    "payload": {"type": "object"},
                    "tags": {"type": "array", "items": {"type": "string"}},
                    "name": {"type": "string"},
                },
                "required": ["id"],
            },
            "fields": ["id", "created_at", "birthday", "amount", "payload", "tags", "name"],
            "field_types": {
                "id": "uuid",
                "created_at": "timestamp",
                "birthday": "date",
                "amount": "integer",
                "payload": "json",
                "tags": "array",
                "name": "string",
            },
            "required_fields": ["id"],
            "primary_keys": ["id"],
            "business_keys": [],
            "hash_keys": [],
        }
    )

    assert isinstance(_sqlalchemy_type_from_contract_field(contract, "id"), PGUUID)
    assert isinstance(_sqlalchemy_type_from_contract_field(contract, "created_at"), DateTime)
    assert isinstance(_sqlalchemy_type_from_contract_field(contract, "birthday"), Date)
    assert isinstance(_sqlalchemy_type_from_contract_field(contract, "amount"), Integer)
    assert isinstance(_sqlalchemy_type_from_contract_field(contract, "payload"), JSONB)
    assert isinstance(_sqlalchemy_type_from_contract_field(contract, "tags"), JSONB)
    assert isinstance(_sqlalchemy_type_from_contract_field(contract, "name"), Text)
