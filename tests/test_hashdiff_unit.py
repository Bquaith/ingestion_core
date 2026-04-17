from __future__ import annotations

import pytest
from sqlalchemy import Column, MetaData, Numeric, String, Table
from sqlalchemy.dialects.postgresql import JSONB

from ingestion_core.contracts.types import ContractDefinition
from ingestion_core.strategies.hash_diff.engine import (
    _validate_source_columns,
    classify_changes,
    run_hash_diff,
)


def test_contract_definition_uses_primary_keys_first() -> None:
    contract = ContractDefinition.from_registry_payload(
        {
            "contract_id": "c1",
            "target_layer": "curated",
            "version": "1",
            "checksum": "abc",
            "fields": ["id", "status", "amount"],
            "primary_keys": ["id"],
            "business_keys": ["status"],
            "hash_keys": ["status", "amount"],
        }
    )

    assert contract.key_fields == ["id"]
    assert contract.effective_hash_fields == ["status", "amount"]


def test_contract_definition_requires_keys() -> None:
    with pytest.raises(ValueError, match="keys.primary or keys.business"):
        ContractDefinition.from_registry_payload(
            {
                "contract_id": "c1",
                "target_layer": "curated",
                "version": "1",
                "checksum": "abc",
                "fields": ["id", "status"],
                "primary_keys": [],
                "business_keys": [],
                "hash_keys": [],
            }
        )


def test_contract_definition_rejects_unknown_required_fields() -> None:
    with pytest.raises(ValueError, match="Required fields are absent in fields"):
        ContractDefinition.from_registry_payload(
            {
                "contract_id": "c1",
                "target_layer": "curated",
                "version": "1",
                "checksum": "abc",
                "fields": ["id", "status"],
                "required_fields": ["missing_field"],
                "primary_keys": ["id"],
                "business_keys": [],
                "hash_keys": [],
            }
        )


def test_classify_changes_insert_update_unchanged() -> None:
    source_rows = [
        {"id": 1, "status": "NEW", "row_hash": "hash-1"},
        {"id": 2, "status": "PAID", "row_hash": "hash-2-new"},
        {"id": 3, "status": "NEW", "row_hash": "hash-3"},
    ]
    existing_hashes = {
        (1,): "hash-1",
        (2,): "hash-2-old",
    }

    inserts, updates, unchanged = classify_changes(
        source_rows=source_rows,
        existing_hashes=existing_hashes,
        key_fields=["id"],
    )

    assert [row["id"] for row in inserts] == [3]
    assert [row["id"] for row in updates] == [2]
    assert unchanged == 1


def test_run_hash_diff_validates_batch_sizes() -> None:
    contract = ContractDefinition.from_registry_payload(
        {
            "contract_id": "c1",
            "target_layer": "curated",
            "version": "1",
            "checksum": "abc",
            "fields": ["id", "status"],
            "primary_keys": ["id"],
            "business_keys": [],
            "hash_keys": ["status"],
        }
    )

    with pytest.raises(ValueError, match="source_batch_size"):
        run_hash_diff(
            source_dsn="postgresql+psycopg2://user:pass@localhost:5432/source",
            source_table="public.source_table",
            target_dsn="postgresql+psycopg2://user:pass@localhost:5432/target",
            target_table_curated="curated.target_table",
            contract=contract,
            source_batch_size=0,
        )

    with pytest.raises(ValueError, match="upsert_batch_size"):
        run_hash_diff(
            source_dsn="postgresql+psycopg2://user:pass@localhost:5432/source",
            source_table="public.source_table",
            target_dsn="postgresql+psycopg2://user:pass@localhost:5432/target",
            target_table_curated="curated.target_table",
            contract=contract,
            upsert_batch_size=0,
        )


def test_validate_source_columns_fails_on_type_mismatch() -> None:
    source_table = Table(
        "orders",
        MetaData(),
        Column("order_id", String, nullable=False),
        Column("amount", String, nullable=False),
    )

    with pytest.raises(ValueError, match="Source column type mismatch against contract"):
        _validate_source_columns(
            source_table=source_table,
            fields=["order_id", "amount"],
            field_types={"order_id": "string", "amount": "decimal"},
        )


def test_validate_source_columns_accepts_matching_types() -> None:
    source_table = Table(
        "orders",
        MetaData(),
        Column("order_id", String, nullable=False),
        Column("amount", Numeric(12, 2), nullable=False),
    )

    _validate_source_columns(
        source_table=source_table,
        fields=["order_id", "amount"],
        field_types={"order_id": "string", "amount": "decimal"},
    )


def test_validate_source_columns_accepts_json_columns_for_array_contracts() -> None:
    source_table = Table(
        "orders",
        MetaData(),
        Column("order_id", String, nullable=False),
        Column("tags", JSONB, nullable=True),
    )

    _validate_source_columns(
        source_table=source_table,
        fields=["order_id", "tags"],
        field_types={"order_id": "string", "tags": "array"},
    )
