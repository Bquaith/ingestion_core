from __future__ import annotations

import json
import uuid
from typing import Any, Mapping

from sqlalchemy import text
from sqlalchemy.engine import Engine


def ensure_audit_tables(engine: Engine) -> None:
    statements = [
        """
        CREATE SCHEMA IF NOT EXISTS ingestion_meta
        """,
        """
        CREATE TABLE IF NOT EXISTS ingestion_meta.pipeline_state (
            pipeline_id text PRIMARY KEY,
            last_run_at timestamptz,
            last_success_at timestamptz,
            last_status text,
            last_error text,
            updated_at timestamptz NOT NULL DEFAULT now()
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS ingestion_meta.run_audit (
            run_id uuid PRIMARY KEY,
            pipeline_id text NOT NULL,
            contract_id text NOT NULL,
            version text NOT NULL,
            checksum text NOT NULL,
            started_at timestamptz NOT NULL,
            finished_at timestamptz,
            read_count integer NOT NULL DEFAULT 0,
            insert_count integer NOT NULL DEFAULT 0,
            update_count integer NOT NULL DEFAULT 0,
            unchanged_count integer NOT NULL DEFAULT 0,
            status text NOT NULL,
            metrics_json jsonb,
            error_text text
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS ingestion_meta.pipeline_lock (
            pipeline_id text PRIMARY KEY,
            run_id uuid NOT NULL,
            locked_at timestamptz NOT NULL DEFAULT now(),
            lock_until timestamptz NOT NULL
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS ingestion_meta.pipeline_checkpoint (
            pipeline_id text PRIMARY KEY,
            run_id uuid NOT NULL,
            checkpoint_json jsonb NOT NULL,
            updated_at timestamptz NOT NULL DEFAULT now()
        )
        """,
    ]

    with engine.begin() as conn:
        for statement in statements:
            conn.execute(text(statement))
        conn.execute(
            text(
                """
                ALTER TABLE ingestion_meta.run_audit
                ADD COLUMN IF NOT EXISTS metrics_json jsonb
                """
            )
        )


def start_run_audit(
    engine: Engine,
    pipeline_id: str,
    contract_id: str,
    version: str,
    checksum: str,
) -> str:
    run_id = str(uuid.uuid4())

    with engine.begin() as conn:
        conn.execute(
            text(
                """
                INSERT INTO ingestion_meta.run_audit (
                    run_id,
                    pipeline_id,
                    contract_id,
                    version,
                    checksum,
                    started_at,
                    status
                ) VALUES (
                    :run_id,
                    :pipeline_id,
                    :contract_id,
                    :version,
                    :checksum,
                    now(),
                    'running'
                )
                """
            ),
            {
                "run_id": run_id,
                "pipeline_id": pipeline_id,
                "contract_id": contract_id,
                "version": version,
                "checksum": checksum,
            },
        )

    return run_id


def acquire_pipeline_lock(
    engine: Engine,
    pipeline_id: str,
    run_id: str,
    ttl_seconds: int = 7200,
) -> bool:
    with engine.begin() as conn:
        lock_row = conn.execute(
            text(
                """
                INSERT INTO ingestion_meta.pipeline_lock (
                    pipeline_id,
                    run_id,
                    locked_at,
                    lock_until
                ) VALUES (
                    :pipeline_id,
                    :run_id,
                    now(),
                    now() + make_interval(secs => :ttl_seconds)
                )
                ON CONFLICT (pipeline_id)
                DO UPDATE SET
                    run_id = EXCLUDED.run_id,
                    locked_at = now(),
                    lock_until = EXCLUDED.lock_until
                WHERE ingestion_meta.pipeline_lock.lock_until <= now()
                RETURNING run_id
                """
            ),
            {
                "pipeline_id": pipeline_id,
                "run_id": run_id,
                "ttl_seconds": ttl_seconds,
            },
        ).scalar_one_or_none()

    return lock_row is not None


def release_pipeline_lock(engine: Engine, pipeline_id: str, run_id: str) -> None:
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                DELETE FROM ingestion_meta.pipeline_lock
                WHERE pipeline_id = :pipeline_id
                  AND run_id = :run_id
                """
            ),
            {
                "pipeline_id": pipeline_id,
                "run_id": run_id,
            },
        )


def persist_pipeline_checkpoint(
    engine: Engine,
    pipeline_id: str,
    run_id: str,
    checkpoint_payload: Mapping[str, Any],
) -> None:
    checkpoint_json = json.dumps(dict(checkpoint_payload), ensure_ascii=True, sort_keys=True)

    with engine.begin() as conn:
        conn.execute(
            text(
                """
                INSERT INTO ingestion_meta.pipeline_checkpoint (
                    pipeline_id,
                    run_id,
                    checkpoint_json,
                    updated_at
                ) VALUES (
                    :pipeline_id,
                    :run_id,
                    :checkpoint_json::jsonb,
                    now()
                )
                ON CONFLICT (pipeline_id)
                DO UPDATE SET
                    run_id = EXCLUDED.run_id,
                    checkpoint_json = EXCLUDED.checkpoint_json,
                    updated_at = now()
                """
            ),
            {
                "pipeline_id": pipeline_id,
                "run_id": run_id,
                "checkpoint_json": checkpoint_json,
            },
        )


def finish_run_audit(
    engine: Engine,
    run_id: str,
    status: str,
    read_count: int,
    insert_count: int,
    update_count: int,
    unchanged_count: int,
    metrics_json: Mapping[str, Any] | None = None,
    error_text: str | None = None,
) -> None:
    metrics_payload = json.dumps(dict(metrics_json or {}), ensure_ascii=True, sort_keys=True)

    with engine.begin() as conn:
        conn.execute(
            text(
                """
                UPDATE ingestion_meta.run_audit
                SET finished_at = now(),
                    read_count = :read_count,
                    insert_count = :insert_count,
                    update_count = :update_count,
                    unchanged_count = :unchanged_count,
                    status = :status,
                    metrics_json = :metrics_json::jsonb,
                    error_text = :error_text
                WHERE run_id = :run_id
                """
            ),
            {
                "run_id": run_id,
                "read_count": read_count,
                "insert_count": insert_count,
                "update_count": update_count,
                "unchanged_count": unchanged_count,
                "status": status,
                "metrics_json": metrics_payload,
                "error_text": error_text,
            },
        )


def finalize_pipeline_state(engine: Engine, pipeline_id: str) -> None:
    with engine.begin() as conn:
        latest = conn.execute(
            text(
                """
                SELECT started_at, finished_at, status, error_text
                FROM ingestion_meta.run_audit
                WHERE pipeline_id = :pipeline_id
                ORDER BY started_at DESC
                LIMIT 1
                """
            ),
            {"pipeline_id": pipeline_id},
        ).mappings().first()

        if latest is None:
            return

        last_run_at = latest["finished_at"] or latest["started_at"]
        last_success_at = last_run_at if latest["status"] == "success" else None
        last_error = None if latest["status"] == "success" else latest["error_text"]

        conn.execute(
            text(
                """
                INSERT INTO ingestion_meta.pipeline_state (
                    pipeline_id,
                    last_run_at,
                    last_success_at,
                    last_status,
                    last_error,
                    updated_at
                ) VALUES (
                    :pipeline_id,
                    :last_run_at,
                    :last_success_at,
                    :last_status,
                    :last_error,
                    now()
                )
                ON CONFLICT (pipeline_id)
                DO UPDATE SET
                    last_run_at = EXCLUDED.last_run_at,
                    last_success_at = CASE
                        WHEN EXCLUDED.last_status = 'success'
                        THEN EXCLUDED.last_run_at
                        ELSE ingestion_meta.pipeline_state.last_success_at
                    END,
                    last_status = EXCLUDED.last_status,
                    last_error = EXCLUDED.last_error,
                    updated_at = now()
                """
            ),
            {
                "pipeline_id": pipeline_id,
                "last_run_at": last_run_at,
                "last_success_at": last_success_at,
                "last_status": latest["status"],
                "last_error": last_error,
            },
        )
