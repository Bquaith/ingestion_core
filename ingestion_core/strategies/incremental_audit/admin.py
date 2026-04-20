from __future__ import annotations

from sqlalchemy import text

from ingestion_core.adapters.postgres import create_sqlalchemy_engine, parse_table_name, table_exists
from ingestion_core.contracts.types import ContractDefinition
from ingestion_core.strategies.incremental_audit.sql import (
    build_create_audit_function_sql,
    build_create_audit_indexes_sql,
    build_create_audit_schema_sql,
    build_create_audit_table_sql,
    build_create_trigger_sql,
    build_drop_trigger_sql,
    make_audit_trigger_function_name,
    make_audit_trigger_name,
)
from ingestion_core.strategies.incremental_audit.types import (
    AuditSetupResult,
    WATERMARK_MODE_AUTO,
    WATERMARK_MODE_COMMIT_TIMESTAMP,
    WATERMARK_MODE_RECORDED_AT,
)


def resolve_watermark_mode(
    source_admin_dsn: str,
    requested_mode: str = WATERMARK_MODE_AUTO,
) -> str:
    if requested_mode not in {
        WATERMARK_MODE_AUTO,
        WATERMARK_MODE_COMMIT_TIMESTAMP,
        WATERMARK_MODE_RECORDED_AT,
    }:
        raise ValueError(f"Unsupported watermark_mode: {requested_mode}")

    if requested_mode == WATERMARK_MODE_RECORDED_AT:
        return WATERMARK_MODE_RECORDED_AT

    engine = create_sqlalchemy_engine(source_admin_dsn)
    try:
        with engine.connect() as conn:
            track_commit_timestamp = str(
                conn.execute(text("SELECT current_setting('track_commit_timestamp', true)")).scalar_one() or ""
            ).strip().lower()
    finally:
        engine.dispose()

    commit_timestamp_available = track_commit_timestamp in {"on", "true", "1"}

    if requested_mode == WATERMARK_MODE_COMMIT_TIMESTAMP and not commit_timestamp_available:
        raise ValueError(
            "Watermark mode 'commit_timestamp' requires PostgreSQL setting track_commit_timestamp=on"
        )

    if commit_timestamp_available:
        return WATERMARK_MODE_COMMIT_TIMESTAMP
    return WATERMARK_MODE_RECORDED_AT


def ensure_source_audit_capture(
    source_admin_dsn: str,
    source_table: str,
    source_audit_table: str,
    contract: ContractDefinition,
    watermark_mode: str = WATERMARK_MODE_AUTO,
    replace_existing_trigger: bool = False,
) -> AuditSetupResult:
    source_schema, source_name = parse_table_name(source_table)
    audit_schema, audit_table = parse_table_name(source_audit_table)
    effective_watermark_mode = resolve_watermark_mode(source_admin_dsn, watermark_mode)
    source_table_token = f"{source_schema}_{source_name}"
    function_name = make_audit_trigger_function_name(source_table_token)
    trigger_name = make_audit_trigger_name(source_table_token)

    engine = create_sqlalchemy_engine(source_admin_dsn)
    audit_schema_created = False
    audit_table_created = False
    trigger_function_created = False
    trigger_created = False
    index_count_created = 0

    try:
        with engine.begin() as conn:
            schema_exists = bool(
                conn.execute(
                    text("SELECT EXISTS (SELECT 1 FROM information_schema.schemata WHERE schema_name = :schema_name)"),
                    {"schema_name": audit_schema},
                ).scalar_one()
            )
            if not schema_exists:
                conn.execute(text(build_create_audit_schema_sql(audit_schema)))
                audit_schema_created = True

            audit_table_exists = table_exists(engine, audit_schema, audit_table)
            if not audit_table_exists:
                conn.execute(text(build_create_audit_table_sql(audit_schema, audit_table)))
                audit_table_created = True

            function_exists = bool(
                conn.execute(
                    text(
                        """
                        SELECT EXISTS (
                            SELECT 1
                            FROM pg_proc p
                            JOIN pg_namespace n ON n.oid = p.pronamespace
                            WHERE n.nspname = :schema_name
                              AND p.proname = :function_name
                        )
                        """
                    ),
                    {"schema_name": audit_schema, "function_name": function_name},
                ).scalar_one()
            )
            conn.execute(
                text(
                    build_create_audit_function_sql(
                        source_table=source_table,
                        audit_schema=audit_schema,
                        audit_table=audit_table,
                        function_name=function_name,
                        key_fields=contract.key_fields,
                    )
                )
            )
            trigger_function_created = not function_exists

            trigger_exists = bool(
                conn.execute(
                    text(
                        """
                        SELECT EXISTS (
                            SELECT 1
                            FROM pg_trigger t
                            JOIN pg_class c ON c.oid = t.tgrelid
                            JOIN pg_namespace n ON n.oid = c.relnamespace
                            WHERE n.nspname = :schema_name
                              AND c.relname = :table_name
                              AND t.tgname = :trigger_name
                              AND NOT t.tgisinternal
                        )
                        """
                    ),
                    {
                        "schema_name": source_schema,
                        "table_name": source_name,
                        "trigger_name": trigger_name,
                    },
                ).scalar_one()
            )
            if trigger_exists and replace_existing_trigger:
                conn.execute(text(build_drop_trigger_sql(source_schema, source_name, trigger_name)))
                trigger_exists = False

            if not trigger_exists:
                conn.execute(
                    text(
                        build_create_trigger_sql(
                            source_schema=source_schema,
                            source_table=source_name,
                            trigger_name=trigger_name,
                            audit_schema=audit_schema,
                            function_name=function_name,
                        )
                    )
                )
                trigger_created = True

            for index_sql in build_create_audit_indexes_sql(audit_schema, audit_table):
                index_name = index_sql.split()[5].strip('"')
                index_exists = bool(
                    conn.execute(
                        text(
                            """
                            SELECT EXISTS (
                                SELECT 1
                                FROM pg_indexes
                                WHERE schemaname = :schema_name
                                  AND tablename = :table_name
                                  AND indexname = :index_name
                            )
                            """
                        ),
                        {
                            "schema_name": audit_schema,
                            "table_name": audit_table,
                            "index_name": index_name,
                        },
                    ).scalar_one()
                )
                conn.execute(text(index_sql))
                if not index_exists:
                    index_count_created += 1
    finally:
        engine.dispose()

    return AuditSetupResult(
        watermark_mode=effective_watermark_mode,
        audit_schema_created=audit_schema_created,
        audit_table_created=audit_table_created,
        trigger_function_created=trigger_function_created,
        trigger_created=trigger_created,
        index_count_created=index_count_created,
    )
