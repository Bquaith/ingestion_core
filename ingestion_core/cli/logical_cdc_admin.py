from __future__ import annotations

import argparse
import sys
from typing import Sequence

from ingestion_core.cli._shared import write_json_payload, add_json_output_arguments
from ingestion_core.contracts.types import ContractDefinition
from ingestion_core.strategies.logical_cdc import (
    OUTPUT_PLUGIN_PGOUTPUT,
    REPLICA_IDENTITY_DEFAULT,
    REPLICA_IDENTITY_FULL,
    ensure_source_logical_cdc_capture,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="ingestion-ensure-logical-cdc",
        description="Configure source PostgreSQL publication, slot, and replica identity for logical CDC.",
    )
    parser.add_argument(
        "--source-admin-dsn",
        required=True,
        help="Administrative SQLAlchemy DSN for the source PostgreSQL instance.",
    )
    parser.add_argument(
        "--source-table",
        required=True,
        help="Qualified source table name, for example public.orders.",
    )
    parser.add_argument(
        "--source-publication-name",
        required=True,
        help="Publication name to create or validate.",
    )
    parser.add_argument(
        "--source-slot-name",
        required=True,
        help="Logical replication slot name to create or validate.",
    )
    parser.add_argument(
        "--output-plugin",
        choices=(OUTPUT_PLUGIN_PGOUTPUT,),
        default=OUTPUT_PLUGIN_PGOUTPUT,
        help="Logical decoding output plugin. Only pgoutput is supported.",
    )
    parser.add_argument(
        "--replace-existing-publication",
        action="store_true",
        help="Drop and recreate the publication if it already exists.",
    )
    parser.add_argument(
        "--no-create-slot",
        dest="create_slot_if_missing",
        action="store_false",
        help="Fail if the logical replication slot does not already exist.",
    )
    parser.set_defaults(create_slot_if_missing=True)
    parser.add_argument(
        "--replica-identity-mode",
        choices=(REPLICA_IDENTITY_DEFAULT, REPLICA_IDENTITY_FULL),
        default=REPLICA_IDENTITY_DEFAULT,
        help="Replica identity mode to apply to the source table.",
    )
    parser.add_argument(
        "--auto-configure-wal-settings",
        action="store_true",
        help="Allow ALTER SYSTEM for wal_level/max_replication_slots/max_wal_senders when needed.",
    )
    parser.add_argument(
        "--desired-max-replication-slots",
        type=int,
        default=10,
        help="Desired value to write when max_replication_slots is not configured.",
    )
    parser.add_argument(
        "--desired-max-wal-senders",
        type=int,
        default=10,
        help="Desired value to write when max_wal_senders is not configured.",
    )
    add_json_output_arguments(parser)
    return parser


def run(argv: Sequence[str] | None = None, *, stdout=None, stderr=None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    stdout = stdout or sys.stdout
    stderr = stderr or sys.stderr

    try:
        result = ensure_source_logical_cdc_capture(
            source_admin_dsn=args.source_admin_dsn,
            source_table=args.source_table,
            source_publication_name=args.source_publication_name,
            source_slot_name=args.source_slot_name,
            contract=_placeholder_contract(),
            output_plugin=args.output_plugin,
            replace_existing_publication=args.replace_existing_publication,
            create_slot_if_missing=args.create_slot_if_missing,
            replica_identity_mode=args.replica_identity_mode,
            auto_configure_wal_settings=args.auto_configure_wal_settings,
            desired_max_replication_slots=args.desired_max_replication_slots,
            desired_max_wal_senders=args.desired_max_wal_senders,
        )
    except Exception as exc:
        stderr.write(f"ERROR: {exc}\n")
        return 1

    write_json_payload(
        {
            **result.to_dict(),
            "source_table": args.source_table,
            "source_publication_name": args.source_publication_name,
            "source_slot_name": args.source_slot_name,
        },
        pretty=args.pretty,
        stdout=stdout,
    )
    return 0


def _placeholder_contract() -> ContractDefinition:
    return ContractDefinition.from_registry_payload(
        {
            "contract_id": "logical-cdc-admin-placeholder",
            "target_layer": "curated",
            "version": "0",
            "checksum": "placeholder",
            "schema_json": {
                "type": "object",
                "properties": {
                    "_placeholder_key": {"type": "string"},
                },
                "required": ["_placeholder_key"],
                "additionalProperties": False,
                "x-primaryKey": ["_placeholder_key"],
            },
            "fields": ["_placeholder_key"],
            "field_types": {"_placeholder_key": "string"},
            "required_fields": ["_placeholder_key"],
            "primary_keys": ["_placeholder_key"],
            "business_keys": [],
            "hash_keys": [],
        }
    )


def main() -> int:
    return run()


if __name__ == "__main__":
    raise SystemExit(main())
