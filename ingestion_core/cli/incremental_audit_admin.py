from __future__ import annotations

import argparse
import sys
from typing import Sequence

from ingestion_core.cli._shared import (
    add_contract_source_arguments,
    add_json_output_arguments,
    load_contract_definition_from_args,
    write_json_payload,
)
from ingestion_core.strategies.incremental_audit import ensure_source_audit_capture


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="ingestion-ensure-incremental-audit",
        description="Configure source PostgreSQL audit trigger capture for incremental ingestion.",
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
        "--source-audit-table",
        required=True,
        help="Qualified audit table name, for example ingestion_meta.orders_audit.",
    )
    parser.add_argument(
        "--watermark-mode",
        choices=("auto", "commit_timestamp", "recorded_at"),
        default="auto",
        help="Watermark strategy for later extract windows.",
    )
    parser.add_argument(
        "--replace-existing-trigger",
        action="store_true",
        help="Drop and recreate the existing trigger if it is already present.",
    )
    add_contract_source_arguments(parser)
    add_json_output_arguments(parser)
    return parser


def run(argv: Sequence[str] | None = None, *, stdout=None, stderr=None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    stdout = stdout or sys.stdout
    stderr = stderr or sys.stderr

    try:
        contract = load_contract_definition_from_args(args)
        result = ensure_source_audit_capture(
            source_admin_dsn=args.source_admin_dsn,
            source_table=args.source_table,
            source_audit_table=args.source_audit_table,
            contract=contract,
            watermark_mode=args.watermark_mode,
            replace_existing_trigger=args.replace_existing_trigger,
        )
    except Exception as exc:
        stderr.write(f"ERROR: {exc}\n")
        return 1

    write_json_payload(
        {
            **result.to_dict(),
            "contract_id": contract.contract_id,
            "contract_version": contract.version,
            "source_table": args.source_table,
            "source_audit_table": args.source_audit_table,
            "key_fields": list(contract.key_fields),
        },
        pretty=args.pretty,
        stdout=stdout,
    )
    return 0


def main() -> int:
    return run()


if __name__ == "__main__":
    raise SystemExit(main())
