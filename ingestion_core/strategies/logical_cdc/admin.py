from __future__ import annotations

from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError

from ingestion_core.adapters.postgres import create_sqlalchemy_engine, parse_table_name
from ingestion_core.contracts.types import ContractDefinition
from ingestion_core.strategies.logical_cdc.sql import (
    build_alter_system_set_sql,
    build_alter_publication_add_table_sql,
    build_check_logical_cdc_settings_sql,
    build_create_logical_slot_sql,
    build_create_publication_sql,
    build_drop_publication_sql,
    build_publication_exists_sql,
    build_replica_identity_sql,
    build_reload_conf_sql,
    build_slot_info_sql,
    build_table_in_publication_sql,
)
from ingestion_core.strategies.logical_cdc.types import (
    OUTPUT_PLUGIN_PGOUTPUT,
    REPLICA_IDENTITY_DEFAULT,
    REPLICA_IDENTITY_FULL,
    LogicalCdcSetupResult,
)


def _wal_setting_changes(
    wal_level: str,
    max_replication_slots: int,
    max_wal_senders: int,
    desired_max_replication_slots: int,
    desired_max_wal_senders: int,
) -> dict[str, str | int]:
    changes: dict[str, str | int] = {}
    if wal_level != "logical":
        changes["wal_level"] = "logical"
    if max_replication_slots <= 0:
        changes["max_replication_slots"] = desired_max_replication_slots
    if max_wal_senders <= 0:
        changes["max_wal_senders"] = desired_max_wal_senders
    return changes


def _format_settings(settings: dict[str, str | int]) -> str:
    return ", ".join(f"{name}={value}" for name, value in settings.items())


def _configure_wal_settings_with_alter_system(engine, settings: dict[str, str | int]) -> None:
    try:
        with engine.connect().execution_options(isolation_level="AUTOCOMMIT") as conn:
            for setting_name, setting_value in settings.items():
                conn.execute(text(build_alter_system_set_sql(setting_name, setting_value)))
            conn.execute(text(build_reload_conf_sql()))
    except SQLAlchemyError as exc:
        raise ValueError(
            "Source PostgreSQL WAL auto-configuration failed. "
            f"Could not execute ALTER SYSTEM for settings: {_format_settings(settings)}. "
            "Use a superuser/admin connection or configure these settings manually. "
            f"Original error: {exc}"
        ) from exc


def _validate_or_configure_wal_settings(
    engine,
    auto_configure_wal_settings: bool,
    desired_max_replication_slots: int,
    desired_max_wal_senders: int,
) -> tuple[str, int, int]:
    with engine.connect() as conn:
        settings = conn.execute(text(build_check_logical_cdc_settings_sql())).mappings().one()

    wal_level = str(settings["wal_level"])
    max_replication_slots = int(settings["max_replication_slots"])
    max_wal_senders = int(settings["max_wal_senders"])
    changes = _wal_setting_changes(
        wal_level=wal_level,
        max_replication_slots=max_replication_slots,
        max_wal_senders=max_wal_senders,
        desired_max_replication_slots=desired_max_replication_slots,
        desired_max_wal_senders=desired_max_wal_senders,
    )
    if not changes:
        return wal_level, max_replication_slots, max_wal_senders

    current_settings = {
        "wal_level": wal_level,
        "max_replication_slots": max_replication_slots,
        "max_wal_senders": max_wal_senders,
    }
    if not auto_configure_wal_settings:
        raise ValueError(
            "Logical CDC requires source PostgreSQL settings: "
            "wal_level=logical, max_replication_slots>0, max_wal_senders>0. "
            f"Current settings: {_format_settings(current_settings)}. "
            "Set auto_configure_wal_settings=true to write them with ALTER SYSTEM, "
            "then restart PostgreSQL, or configure them manually."
        )

    _configure_wal_settings_with_alter_system(engine, changes)
    raise ValueError(
        "Source PostgreSQL WAL settings were written with ALTER SYSTEM but require PostgreSQL restart "
        "before logical CDC can continue. "
        f"Applied settings: {_format_settings(changes)}. "
        "Restart the source PostgreSQL instance and rerun ingest_contract_logical_cdc."
    )


def _ensure_logical_replication_slot(
    engine,
    source_slot_name: str,
    output_plugin: str,
    create_slot_if_missing: bool,
) -> bool:
    with engine.connect().execution_options(isolation_level="AUTOCOMMIT") as conn:
        slot_info = conn.execute(text(build_slot_info_sql()), {"slot_name": source_slot_name}).mappings().first()
        if slot_info is None:
            if not create_slot_if_missing:
                raise ValueError(f"Logical replication slot {source_slot_name!r} does not exist")
            conn.execute(text(build_create_logical_slot_sql(source_slot_name, output_plugin)))
            return True

        plugin = str(slot_info["plugin"])
        if plugin != output_plugin:
            raise ValueError(
                f"Logical replication slot {source_slot_name!r} uses plugin {plugin!r}, "
                f"expected {output_plugin!r}"
            )
        return False


def ensure_source_logical_cdc_capture(
    source_admin_dsn: str,
    source_table: str,
    source_publication_name: str,
    source_slot_name: str,
    contract: ContractDefinition,
    output_plugin: str = OUTPUT_PLUGIN_PGOUTPUT,
    replace_existing_publication: bool = False,
    create_slot_if_missing: bool = True,
    replica_identity_mode: str = REPLICA_IDENTITY_DEFAULT,
    auto_configure_wal_settings: bool = False,
    desired_max_replication_slots: int = 10,
    desired_max_wal_senders: int = 10,
) -> LogicalCdcSetupResult:
    if output_plugin != OUTPUT_PLUGIN_PGOUTPUT:
        raise ValueError("Only native PostgreSQL pgoutput is supported; wal2json is intentionally unsupported")
    if replica_identity_mode not in {REPLICA_IDENTITY_DEFAULT, REPLICA_IDENTITY_FULL}:
        raise ValueError("replica_identity_mode must be 'default' or 'full'")
    if desired_max_replication_slots <= 0:
        raise ValueError("desired_max_replication_slots must be greater than zero")
    if desired_max_wal_senders <= 0:
        raise ValueError("desired_max_wal_senders must be greater than zero")

    source_schema, source_name = parse_table_name(source_table)
    engine = create_sqlalchemy_engine(source_admin_dsn)
    publication_created = False
    table_added_to_publication = False
    slot_created = False
    replica_identity_changed = False

    try:
        wal_level, max_replication_slots, max_wal_senders = _validate_or_configure_wal_settings(
            engine=engine,
            auto_configure_wal_settings=auto_configure_wal_settings,
            desired_max_replication_slots=desired_max_replication_slots,
            desired_max_wal_senders=desired_max_wal_senders,
        )
        with engine.begin() as conn:
            publication_exists = bool(
                conn.execute(
                    text(build_publication_exists_sql()),
                    {"publication_name": source_publication_name},
                ).scalar_one()
            )
            if publication_exists and replace_existing_publication:
                conn.execute(text(build_drop_publication_sql(source_publication_name)))
                publication_exists = False

            if not publication_exists:
                conn.execute(text(build_create_publication_sql(source_publication_name, source_schema, source_name)))
                publication_created = True
                table_added_to_publication = True
            else:
                table_in_publication = bool(
                    conn.execute(
                        text(build_table_in_publication_sql()),
                        {
                            "publication_name": source_publication_name,
                            "source_schema": source_schema,
                            "source_table": source_name,
                        },
                    ).scalar_one()
                )
                if not table_in_publication:
                    conn.execute(
                        text(build_alter_publication_add_table_sql(source_publication_name, source_schema, source_name))
                    )
                    table_added_to_publication = True

            conn.execute(text(build_replica_identity_sql(source_schema, source_name, replica_identity_mode)))
            replica_identity_changed = True

        slot_created = _ensure_logical_replication_slot(
            engine=engine,
            source_slot_name=source_slot_name,
            output_plugin=output_plugin,
            create_slot_if_missing=create_slot_if_missing,
        )

        return LogicalCdcSetupResult(
            wal_level=wal_level,
            max_replication_slots=max_replication_slots,
            max_wal_senders=max_wal_senders,
            publication_created=publication_created,
            table_added_to_publication=table_added_to_publication,
            slot_created=slot_created,
            replica_identity_changed=replica_identity_changed,
            output_plugin=output_plugin,
        )
    finally:
        engine.dispose()
