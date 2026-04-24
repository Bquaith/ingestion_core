from __future__ import annotations

from ingestion_core.adapters.postgres import validate_identifier
from ingestion_core.strategies.common.delta_apply import qualify_table, quote_identifier
from ingestion_core.strategies.logical_cdc.types import OUTPUT_PLUGIN_PGOUTPUT, REPLICA_IDENTITY_FULL


def validate_logical_name(name: str) -> str:
    return validate_identifier(name)


def build_check_logical_cdc_settings_sql() -> str:
    return """
    SELECT
        current_setting('wal_level') AS wal_level,
        current_setting('max_replication_slots')::int AS max_replication_slots,
        current_setting('max_wal_senders')::int AS max_wal_senders
    """.strip()


def build_alter_system_set_sql(setting_name: str, setting_value: str | int) -> str:
    allowed_settings = {"wal_level", "max_replication_slots", "max_wal_senders"}
    if setting_name not in allowed_settings:
        raise ValueError(f"Unsupported ALTER SYSTEM setting: {setting_name}")
    value = str(setting_value).replace("'", "''")
    return f"ALTER SYSTEM SET {setting_name} = '{value}'"


def build_reload_conf_sql() -> str:
    return "SELECT pg_reload_conf()"


def build_publication_exists_sql() -> str:
    return "SELECT EXISTS (SELECT 1 FROM pg_publication WHERE pubname = :publication_name)"


def build_table_in_publication_sql() -> str:
    return """
    SELECT EXISTS (
        SELECT 1
        FROM pg_publication p
        JOIN pg_publication_rel pr ON pr.prpubid = p.oid
        JOIN pg_class c ON c.oid = pr.prrelid
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE p.pubname = :publication_name
          AND n.nspname = :source_schema
          AND c.relname = :source_table
    )
    """.strip()


def build_create_publication_sql(publication_name: str, source_schema: str, source_table: str) -> str:
    return (
        f"CREATE PUBLICATION {quote_identifier(validate_logical_name(publication_name))} "
        f"FOR TABLE {qualify_table(source_schema, source_table)} "
        "WITH (publish = 'insert, update, delete')"
    )


def build_alter_publication_add_table_sql(publication_name: str, source_schema: str, source_table: str) -> str:
    return (
        f"ALTER PUBLICATION {quote_identifier(validate_logical_name(publication_name))} "
        f"ADD TABLE {qualify_table(source_schema, source_table)}"
    )


def build_drop_publication_sql(publication_name: str) -> str:
    return f"DROP PUBLICATION IF EXISTS {quote_identifier(validate_logical_name(publication_name))}"


def build_slot_info_sql() -> str:
    return """
    SELECT slot_name, plugin, slot_type, active, restart_lsn::text, confirmed_flush_lsn::text
    FROM pg_replication_slots
    WHERE slot_name = :slot_name
    """.strip()


def build_create_logical_slot_sql(slot_name: str, output_plugin: str = OUTPUT_PLUGIN_PGOUTPUT) -> str:
    return (
        "SELECT slot_name, lsn::text "
        "FROM pg_create_logical_replication_slot("
        f"'{validate_logical_name(slot_name)}', "
        f"'{validate_logical_name(output_plugin)}'"
        ")"
    )


def build_replica_identity_sql(source_schema: str, source_table: str, mode: str) -> str:
    normalized_mode = mode.strip().lower()
    if normalized_mode == REPLICA_IDENTITY_FULL:
        return f"ALTER TABLE {qualify_table(source_schema, source_table)} REPLICA IDENTITY FULL"
    return f"ALTER TABLE {qualify_table(source_schema, source_table)} REPLICA IDENTITY DEFAULT"


def build_current_wal_lsn_sql() -> str:
    return "SELECT pg_current_wal_lsn()::text"


def build_slot_lag_sql() -> str:
    return """
    SELECT
        slot_name,
        active,
        restart_lsn::text,
        confirmed_flush_lsn::text,
        pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)::bigint AS retained_wal_bytes
    FROM pg_replication_slots
    WHERE slot_name = :slot_name
    """.strip()
