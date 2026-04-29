from __future__ import annotations

import argparse
import json
import sys
from typing import Any, Iterable, Sequence

from ingestion_core.strategies.logical_cdc.decode import PgOutputDecoder

_SLOT_INFO_SQL = """
SELECT
    slot_name,
    plugin,
    slot_type,
    active,
    active_pid,
    restart_lsn::text,
    confirmed_flush_lsn::text
FROM pg_replication_slots
WHERE slot_name = %s
"""

_PEEK_SQL = """
SELECT lsn::text, xid, data
FROM pg_logical_slot_peek_binary_changes(
    %s,
    NULL,
    %s,
    'proto_version', '1',
    'publication_names', %s,
    'messages', %s
)
"""


class PeekLogicalCdcError(RuntimeError):
    pass


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="ingestion-peek-logical-cdc",
        description="Peek logical CDC pgoutput messages from a PostgreSQL replication slot without advancing it.",
    )
    parser.add_argument("--dsn", required=True, help="Replication-compatible PostgreSQL DSN, for example postgresql://...")
    parser.add_argument("--slot", required=True, help="Logical replication slot name")
    parser.add_argument("--publication", required=True, help="Publication name used by the slot")
    parser.add_argument("--source-table", required=True, help="Qualified table name to decode, for example public.orders")
    parser.add_argument("--limit", type=int, default=100, help="Maximum number of binary slot messages to peek")
    parser.add_argument(
        "--connect-timeout",
        type=int,
        default=5,
        help="PostgreSQL connection timeout in seconds",
    )
    parser.add_argument(
        "--statement-timeout-ms",
        type=int,
        default=10000,
        help="Statement timeout in milliseconds for the peek query",
    )
    parser.add_argument(
        "--messages",
        action="store_true",
        help="Include logical replication messages emitted with pg_logical_emit_message",
    )
    parser.add_argument(
        "--pretty",
        action="store_true",
        help="Pretty-print each decoded event as multi-line JSON instead of NDJSON",
    )
    return parser


def get_slot_info(
    conn,
    slot_name: str,
) -> dict[str, Any]:
    with conn.cursor() as cur:
        cur.execute(_SLOT_INFO_SQL, (slot_name,))
        row = cur.fetchone()
    if row is None:
        raise PeekLogicalCdcError(f"Logical replication slot {slot_name!r} does not exist")
    return {
        "slot_name": row[0],
        "plugin": row[1],
        "slot_type": row[2],
        "active": bool(row[3]),
        "active_pid": row[4],
        "restart_lsn": row[5],
        "confirmed_flush_lsn": row[6],
    }


def peek_binary_change_rows(
    dsn: str,
    slot_name: str,
    publication_name: str,
    limit: int,
    include_messages: bool = False,
    connect_timeout: int = 5,
    statement_timeout_ms: int = 10000,
) -> list[tuple[str | None, int | None, bytes]]:
    import psycopg2
    from psycopg2 import errors as psycopg_errors

    conn = None
    try:
        conn = psycopg2.connect(
            dsn,
            connect_timeout=connect_timeout,
            options=f"-c statement_timeout={statement_timeout_ms}",
        )
        slot_info = get_slot_info(conn, slot_name)
        if slot_info["plugin"] != "pgoutput":
            raise PeekLogicalCdcError(
                f"Logical replication slot {slot_name!r} uses plugin {slot_info['plugin']!r}; expected 'pgoutput'"
            )
        if slot_info["slot_type"] != "logical":
            raise PeekLogicalCdcError(
                f"Replication slot {slot_name!r} has slot_type {slot_info['slot_type']!r}; expected 'logical'"
            )
        if slot_info["active"]:
            raise PeekLogicalCdcError(
                f"Logical replication slot {slot_name!r} is active in backend PID {slot_info['active_pid']}. "
                "Stop the running consumer or use another slot before peeking."
            )
        with conn.cursor() as cur:
            try:
                cur.execute(
                    _PEEK_SQL,
                    (
                        slot_name,
                        limit,
                        publication_name,
                        "true" if include_messages else "false",
                    ),
                )
                return [(row[0], row[1], bytes(row[2])) for row in cur.fetchall()]
            except psycopg_errors.QueryCanceled as exc:
                raise PeekLogicalCdcError(
                    "Logical slot peek exceeded statement_timeout while PostgreSQL was preparing logical decoding output. "
                    "This often means the server is waiting in snapshot build (for example SnapbuildSync) or there are "
                    "long-running transactions blocking a consistent decoding snapshot. Increase --statement-timeout-ms "
                    "or inspect pg_stat_activity / long transactions on the source."
                ) from exc
            except psycopg_errors.ObjectInUse as exc:
                raise PeekLogicalCdcError(
                    f"Logical replication slot {slot_name!r} is already active. "
                    "Stop the current consumer and retry."
                ) from exc
    except KeyboardInterrupt as exc:
        if conn is not None:
            try:
                conn.cancel()
            except Exception:
                pass
        raise PeekLogicalCdcError("Interrupted by user while peeking logical CDC messages") from exc
    finally:
        if conn is not None:
            conn.close()


def decode_peek_rows(
    rows: Iterable[tuple[str | None, int | None, bytes]],
    *,
    source_table: str,
) -> list[dict[str, Any]]:
    decoder = PgOutputDecoder(source_table=source_table)
    decoded_events: list[dict[str, Any]] = []
    for message_lsn, message_xid, payload in rows:
        events = decoder.decode_message(payload)
        for event in events:
            decoded_events.append(
                {
                    "message_lsn": message_lsn,
                    "message_xid": message_xid,
                    "source_op": event.source_op,
                    "commit_lsn": event.commit_lsn,
                    "end_lsn": event.end_lsn,
                    "change_index": event.change_index,
                    "xid": event.xid,
                    "commit_ts": event.commit_ts.isoformat() if event.commit_ts is not None else None,
                    "relation": event.relation.qualified_name,
                    "old_key": dict(event.old_key) if event.old_key is not None else None,
                    "row_after": dict(event.row_after) if event.row_after is not None else None,
                }
            )
    return decoded_events


def _write_events(
    events: Sequence[dict[str, Any]],
    *,
    pretty: bool,
    stdout,
) -> None:
    for event in events:
        if pretty:
            stdout.write(json.dumps(event, ensure_ascii=True, indent=2))
            stdout.write("\n")
        else:
            stdout.write(json.dumps(event, ensure_ascii=True))
            stdout.write("\n")


def run(argv: Sequence[str] | None = None, *, stdout=None, stderr=None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    if args.limit <= 0:
        parser.error("--limit must be greater than zero")
    if args.connect_timeout <= 0:
        parser.error("--connect-timeout must be greater than zero")
    if args.statement_timeout_ms <= 0:
        parser.error("--statement-timeout-ms must be greater than zero")

    stdout = stdout or sys.stdout
    stderr = stderr or sys.stderr

    try:
        rows = peek_binary_change_rows(
            dsn=args.dsn,
            slot_name=args.slot,
            publication_name=args.publication,
            limit=args.limit,
            include_messages=args.messages,
            connect_timeout=args.connect_timeout,
            statement_timeout_ms=args.statement_timeout_ms,
        )
        events = decode_peek_rows(rows, source_table=args.source_table)
    except PeekLogicalCdcError as exc:
        stderr.write(f"ERROR: {exc}\n")
        return 1

    _write_events(events, pretty=args.pretty, stdout=stdout)

    stderr.write(
        "peeked_messages={peeked} decoded_events={decoded} slot={slot} publication={publication} source_table={source_table}\n".format(
            peeked=len(rows),
            decoded=len(events),
            slot=args.slot,
            publication=args.publication,
            source_table=args.source_table,
        )
    )
    if rows and not events:
        stderr.write(
            "decoded_events=0 while peeked_messages>0; the peek window may contain only relation/begin frames or a partial transaction without COMMIT. Increase --limit and retry.\n"
        )
    return 0


def main() -> int:
    return run()


if __name__ == "__main__":
    raise SystemExit(main())
