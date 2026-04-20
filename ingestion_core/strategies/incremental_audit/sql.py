from __future__ import annotations

import hashlib
from typing import Iterable

from ingestion_core.adapters.postgres import validate_identifier
from ingestion_core.strategies.incremental_audit.types import (
    WATERMARK_MODE_COMMIT_TIMESTAMP,
    WATERMARK_MODE_RECORDED_AT,
)


def quote_identifier(name: str) -> str:
    return f'"{validate_identifier(name)}"'


def qualify_table(schema: str, table: str) -> str:
    return f"{quote_identifier(schema)}.{quote_identifier(table)}"


def make_audit_trigger_function_name(source_table_name: str) -> str:
    suffix = hashlib.sha1(source_table_name.encode("utf-8")).hexdigest()[:10]
    return validate_identifier(f"_ia_fn_{source_table_name}_{suffix}"[:63])


def make_audit_trigger_name(source_table_name: str) -> str:
    suffix = hashlib.sha1(source_table_name.encode("utf-8")).hexdigest()[:10]
    return validate_identifier(f"_ia_trg_{source_table_name}_{suffix}"[:63])


def ordering_expression(watermark_mode: str) -> str:
    if watermark_mode == WATERMARK_MODE_COMMIT_TIMESTAMP:
        return "COALESCE(pg_xact_commit_timestamp(source_txid), recorded_at)"
    if watermark_mode == WATERMARK_MODE_RECORDED_AT:
        return "recorded_at"
    raise ValueError(f"Unsupported watermark_mode: {watermark_mode}")


def build_create_audit_schema_sql(audit_schema: str) -> str:
    return f"CREATE SCHEMA IF NOT EXISTS {quote_identifier(audit_schema)}"


def build_create_audit_table_sql(audit_schema: str, audit_table: str) -> str:
    return f"""
    CREATE TABLE IF NOT EXISTS {qualify_table(audit_schema, audit_table)} (
        audit_event_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
        source_table TEXT NOT NULL,
        op TEXT NOT NULL CHECK (op IN ('I', 'U', 'D')),
        source_txid BIGINT NOT NULL,
        recorded_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
        key_json JSONB NOT NULL,
        row_before JSONB,
        row_after JSONB,
        changed_columns TEXT[]
    )
    """.strip()


def build_jsonb_key_expression(key_fields: Iterable[str], record_name: str) -> str:
    parts: list[str] = []
    for field in key_fields:
        safe_field = validate_identifier(field)
        parts.append(f"'{safe_field}'")
        parts.append(f"{record_name}.{quote_identifier(safe_field)}")
    if not parts:
        raise ValueError("key_fields must not be empty")
    return f"jsonb_build_object({', '.join(parts)})"


def build_create_audit_function_sql(
    source_table: str,
    audit_schema: str,
    audit_table: str,
    function_name: str,
    key_fields: list[str],
) -> str:
    del source_table
    new_key_expr = build_jsonb_key_expression(key_fields, "NEW")
    old_key_expr = build_jsonb_key_expression(key_fields, "OLD")
    audit_table_ref = qualify_table(audit_schema, audit_table)
    function_ref = qualify_table(audit_schema, function_name)

    return f"""
    CREATE OR REPLACE FUNCTION {function_ref}()
    RETURNS trigger
    LANGUAGE plpgsql
    AS $$
    DECLARE
        v_key_json jsonb;
        v_changed_columns text[];
    BEGIN
        IF TG_OP = 'INSERT' THEN
            v_key_json := {new_key_expr};
            INSERT INTO {audit_table_ref} (
                source_table,
                op,
                source_txid,
                key_json,
                row_before,
                row_after,
                changed_columns
            )
            VALUES (
                TG_TABLE_SCHEMA || '.' || TG_TABLE_NAME,
                'I',
                txid_current(),
                v_key_json,
                NULL,
                to_jsonb(NEW),
                NULL
            );
            RETURN NEW;
        ELSIF TG_OP = 'UPDATE' THEN
            v_key_json := {new_key_expr};
            SELECT COALESCE(array_agg(key ORDER BY key), ARRAY[]::text[])
            INTO v_changed_columns
            FROM (
                SELECT key
                FROM jsonb_each(to_jsonb(NEW))
                WHERE (to_jsonb(OLD) -> key) IS DISTINCT FROM value
            ) AS changed;

            INSERT INTO {audit_table_ref} (
                source_table,
                op,
                source_txid,
                key_json,
                row_before,
                row_after,
                changed_columns
            )
            VALUES (
                TG_TABLE_SCHEMA || '.' || TG_TABLE_NAME,
                'U',
                txid_current(),
                v_key_json,
                to_jsonb(OLD),
                to_jsonb(NEW),
                v_changed_columns
            );
            RETURN NEW;
        ELSIF TG_OP = 'DELETE' THEN
            v_key_json := {old_key_expr};
            INSERT INTO {audit_table_ref} (
                source_table,
                op,
                source_txid,
                key_json,
                row_before,
                row_after,
                changed_columns
            )
            VALUES (
                TG_TABLE_SCHEMA || '.' || TG_TABLE_NAME,
                'D',
                txid_current(),
                v_key_json,
                to_jsonb(OLD),
                NULL,
                NULL
            );
            RETURN OLD;
        END IF;
        RETURN NULL;
    END;
    $$;
    """.strip()


def build_drop_trigger_sql(source_schema: str, source_table: str, trigger_name: str) -> str:
    return (
        f"DROP TRIGGER IF EXISTS {quote_identifier(trigger_name)} "
        f"ON {qualify_table(source_schema, source_table)}"
    )


def build_create_trigger_sql(
    source_schema: str,
    source_table: str,
    trigger_name: str,
    audit_schema: str,
    function_name: str,
) -> str:
    return f"""
    CREATE TRIGGER {quote_identifier(trigger_name)}
    AFTER INSERT OR UPDATE OR DELETE
    ON {qualify_table(source_schema, source_table)}
    FOR EACH ROW
    EXECUTE FUNCTION {qualify_table(audit_schema, function_name)}()
    """.strip()


def build_create_audit_indexes_sql(audit_schema: str, audit_table: str) -> list[str]:
    return [
        (
            f"CREATE INDEX IF NOT EXISTS {quote_identifier(f'idx_{audit_table}_recorded_event'[:63])} "
            f"ON {qualify_table(audit_schema, audit_table)} (recorded_at, audit_event_id)"
        ),
        (
            f"CREATE INDEX IF NOT EXISTS {quote_identifier(f'idx_{audit_table}_txid_event'[:63])} "
            f"ON {qualify_table(audit_schema, audit_table)} (source_txid, audit_event_id)"
        ),
    ]


def build_select_latest_watermark_sql(audit_schema: str, audit_table: str, watermark_mode: str) -> str:
    ordering_expr = ordering_expression(watermark_mode)
    return f"""
    SELECT
        {ordering_expr} AS ordering_ts,
        audit_event_id
    FROM {qualify_table(audit_schema, audit_table)}
    ORDER BY ordering_ts DESC, audit_event_id DESC
    LIMIT 1
    """.strip()


def build_select_audit_window_sql(
    audit_schema: str,
    audit_table: str,
    watermark_mode: str,
    has_lower_bound: bool,
) -> str:
    ordering_expr = ordering_expression(watermark_mode)
    where_clauses = [
        "("
        f"{ordering_expr} < :end_ordering_ts "
        f"OR ({ordering_expr} = :end_ordering_ts AND audit_event_id <= :end_event_id)"
        ")"
    ]
    if has_lower_bound:
        where_clauses.append(
            "("
            f"{ordering_expr} > :start_ordering_ts "
            f"OR ({ordering_expr} = :start_ordering_ts AND audit_event_id > :start_event_id)"
            ")"
        )

    return f"""
    SELECT
        audit_event_id,
        op,
        source_txid,
        recorded_at,
        {ordering_expr} AS ordering_ts,
        key_json,
        row_before,
        row_after,
        changed_columns
    FROM {qualify_table(audit_schema, audit_table)}
    WHERE {' AND '.join(where_clauses)}
    ORDER BY ordering_ts ASC, audit_event_id ASC
    """.strip()
