from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal

from ingestion_core.contracts.schema_validation import validate_instance_against_schema


def test_validate_instance_against_schema_accepts_valid_nested_payload() -> None:
    schema = {
        "type": "object",
        "properties": {
            "id": {"type": "string", "format": "uuid"},
            "created_at": {"type": "string", "format": "date-time"},
            "amount": {"type": "number", "minimum": 10, "exclusiveMaximum": 100},
            "status": {"type": "string", "enum": ["NEW", "PAID"]},
            "tags": {
                "type": "array",
                "items": {"type": "string", "minLength": 2},
                "minItems": 1,
                "maxItems": 3,
            },
            "meta": {
                "type": "object",
                "properties": {
                    "region": {"type": "string", "const": "ru"},
                },
                "required": ["region"],
                "additionalProperties": False,
            },
        },
        "required": ["id", "created_at", "amount", "status", "tags", "meta"],
        "additionalProperties": False,
    }

    violations = validate_instance_against_schema(
        schema,
        {
            "id": "5b7650d3-a795-4f1e-aa89-7d29d6ccf29d",
            "created_at": datetime(2025, 1, 1, 10, 0, tzinfo=timezone.utc),
            "amount": Decimal("10.5"),
            "status": "NEW",
            "tags": ["ok", "go"],
            "meta": {"region": "ru"},
        },
    )

    assert violations == []


def test_validate_instance_against_schema_reports_supported_keyword_violations() -> None:
    schema = {
        "type": "object",
        "properties": {
            "id": {"type": "string", "format": "uuid"},
            "status": {"type": "string", "enum": ["NEW", "PAID"], "pattern": "^[A-Z]+$", "minLength": 3},
            "amount": {"type": "number", "minimum": 10, "exclusiveMaximum": 100},
            "tags": {
                "type": "array",
                "items": {"type": "string", "maxLength": 3},
                "minItems": 1,
                "maxItems": 2,
            },
            "meta": {
                "type": "object",
                "properties": {
                    "region": {"type": "string", "const": "ru"},
                },
                "required": ["region"],
                "additionalProperties": False,
            },
        },
        "required": ["id", "status", "amount", "tags", "meta", "missing_required"],
        "additionalProperties": False,
    }

    violations = validate_instance_against_schema(
        schema,
        {
            "id": "not-a-uuid",
            "status": "ok",
            "amount": Decimal("100"),
            "tags": ["toolong", "ok", "x"],
            "meta": {"region": "eu", "extra": "x"},
            "root_extra": True,
        },
    )

    actual = {(violation.code, violation.field) for violation in violations}

    assert ("schema.format", "id") in actual
    assert ("schema.enum", "status") in actual
    assert ("schema.pattern", "status") in actual
    assert ("schema.exclusive_maximum", "amount") in actual
    assert ("schema.max_items", "tags") in actual
    assert ("schema.max_length", "tags[0]") in actual
    assert ("schema.const", "meta.region") in actual
    assert ("schema.additional_properties", "meta.extra") in actual
    assert ("schema.additional_properties", "root_extra") in actual
    assert ("schema.required", "missing_required") in actual


def test_validate_instance_against_schema_exposes_contract_constraint_and_actual_value() -> None:
    schema = {
        "type": "object",
        "properties": {
            "gender": {
                "type": "string",
                "title": "Gender",
                "description": "Allowed values are male or female only",
                "enum": ["male", "female"],
            }
        },
        "required": ["gender"],
        "additionalProperties": False,
    }

    violations = validate_instance_against_schema(schema, {"gender": "femaled"})

    assert len(violations) == 1
    violation = violations[0]
    assert violation.code == "schema.enum"
    assert violation.field == "gender"
    assert violation.constraint == 'enum=["male", "female"]'
    assert violation.actual_value == '"femaled"'
    assert violation.contract_title == "Gender"
    assert violation.contract_description == "Allowed values are male or female only"
    assert 'Constraint: enum=["male", "female"].' in violation.message
    assert 'Reason: value is not included in the allowed set.' in violation.message
    assert 'Actual value: "femaled".' in violation.message
    assert "Contract description: Allowed values are male or female only." in violation.message


def test_validate_instance_against_schema_treats_numeric_enum_and_const_as_decimal_safe() -> None:
    schema = {
        "type": "object",
        "properties": {
            "amount": {"type": "number", "enum": [1.1, 2.2], "const": 1.1},
        },
        "required": ["amount"],
        "additionalProperties": False,
    }

    violations = validate_instance_against_schema(schema, {"amount": Decimal("1.1")})

    assert violations == []


def test_validate_instance_against_schema_skips_legacy_non_json_schema_payload() -> None:
    violations = validate_instance_against_schema(
        {
            "fields": [{"name": "id", "type": "string"}],
            "keys": {"primary": ["id"]},
        },
        {"id": "1"},
    )

    assert violations == []
