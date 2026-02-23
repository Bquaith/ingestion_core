from __future__ import annotations

import os
import uuid

import pytest
from sqlalchemy import text

from ingestion_platform.hash_diff import ContractDefinition, run_hash_diff
from ingestion_platform.postgres import create_sqlalchemy_engine

TEST_SOURCE_DSN = os.getenv("TEST_SOURCE_DSN")
TEST_TARGET_DSN = os.getenv("TEST_TARGET_DSN")


@pytest.mark.integration
def test_hashdiff_two_runs_insert_then_update_and_unchanged() -> None:
    if not TEST_SOURCE_DSN or not TEST_TARGET_DSN:
        pytest.skip("Set TEST_SOURCE_DSN and TEST_TARGET_DSN to run integration test")

    suffix = uuid.uuid4().hex[:8]
    source_table_name = f"orders_it_{suffix}"
    target_table_name = f"orders_curated_it_{suffix}"

    source_fqn = f"public.{source_table_name}"
    target_fqn = f"curated.{target_table_name}"

    source_engine = create_sqlalchemy_engine(TEST_SOURCE_DSN)
    target_engine = create_sqlalchemy_engine(TEST_TARGET_DSN)

    contract = ContractDefinition.from_registry_payload(
        {
            "contract_id": "orders-contract",
            "target_layer": "curated",
            "version": "1",
            "checksum": "checksum-v1",
            "fields": ["order_id", "customer_id", "amount", "status", "updated_at"],
            "primary_keys": ["order_id"],
            "business_keys": [],
            "hash_keys": ["customer_id", "amount", "status", "updated_at"],
        }
    )

    try:
        with source_engine.begin() as conn:
            conn.execute(text(f'DROP TABLE IF EXISTS "public"."{source_table_name}"'))
            conn.execute(
                text(
                    f'''
                    CREATE TABLE "public"."{source_table_name}" (
                        order_id BIGINT PRIMARY KEY,
                        customer_id BIGINT NOT NULL,
                        amount NUMERIC(12,2) NOT NULL,
                        status TEXT NOT NULL,
                        updated_at TIMESTAMPTZ NOT NULL
                    )
                    '''
                )
            )
            conn.execute(
                text(
                    f'''
                    INSERT INTO "public"."{source_table_name}" (
                        order_id, customer_id, amount, status, updated_at
                    ) VALUES
                        (1, 101, 100.00, 'NEW',  '2025-01-01T10:00:00+00:00'),
                        (2, 102, 120.50, 'PAID', '2025-01-01T10:05:00+00:00'),
                        (3, 103, 90.99,  'NEW',  '2025-01-01T10:10:00+00:00')
                    '''
                )
            )

        first = run_hash_diff(
            source_dsn=TEST_SOURCE_DSN,
            source_table=source_fqn,
            target_dsn=TEST_TARGET_DSN,
            target_table_curated=target_fqn,
            contract=contract,
            source_batch_size=2,
            upsert_batch_size=2,
        )

        assert first.read_count == 3
        assert first.insert_count == 3
        assert first.update_count == 0
        assert first.unchanged_count == 0
        assert first.processed_batches == 2
        assert first.total_seconds >= 0.0

        with source_engine.begin() as conn:
            conn.execute(
                text(
                    f'''
                    UPDATE "public"."{source_table_name}"
                    SET amount = 125.50,
                        updated_at = '2025-01-02T10:05:00+00:00'
                    WHERE order_id = 2
                    '''
                )
            )
            conn.execute(
                text(
                    f'''
                    INSERT INTO "public"."{source_table_name}" (
                        order_id, customer_id, amount, status, updated_at
                    ) VALUES
                        (4, 104, 75.00, 'NEW', '2025-01-02T11:00:00+00:00')
                    '''
                )
            )

        second = run_hash_diff(
            source_dsn=TEST_SOURCE_DSN,
            source_table=source_fqn,
            target_dsn=TEST_TARGET_DSN,
            target_table_curated=target_fqn,
            contract=contract,
            source_batch_size=2,
            upsert_batch_size=2,
        )

        assert second.read_count == 4
        assert second.insert_count == 1
        assert second.update_count == 1
        assert second.unchanged_count == 2
        assert second.processed_batches == 2
        assert second.total_seconds >= 0.0

        with target_engine.connect() as conn:
            total = conn.execute(
                text(f'SELECT COUNT(*) FROM "curated"."{target_table_name}"')
            ).scalar_one()
            amount = conn.execute(
                text(
                    f'''
                    SELECT amount::text
                    FROM "curated"."{target_table_name}"
                    WHERE order_id = 2
                    '''
                )
            ).scalar_one()

        assert total == 4
        assert amount == "125.50"
    finally:
        with source_engine.begin() as conn:
            conn.execute(text(f'DROP TABLE IF EXISTS "public"."{source_table_name}"'))
        with target_engine.begin() as conn:
            conn.execute(text(f'DROP TABLE IF EXISTS "curated"."{target_table_name}"'))

        source_engine.dispose()
        target_engine.dispose()
