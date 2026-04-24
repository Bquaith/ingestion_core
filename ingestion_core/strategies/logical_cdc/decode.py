from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import struct
from typing import Any

from ingestion_core.strategies.logical_cdc.types import (
    LogicalCdcSourceEvent,
    PgOutputRelation,
    PgOutputRelationColumn,
    int_to_lsn,
)

_PG_EPOCH = datetime(2000, 1, 1, tzinfo=timezone.utc)


class PgOutputDecodeError(ValueError):
    pass


class _Reader:
    def __init__(self, payload: bytes) -> None:
        self.payload = payload
        self.offset = 0

    def read(self, size: int) -> bytes:
        end = self.offset + size
        if end > len(self.payload):
            raise PgOutputDecodeError("Unexpected end of pgoutput message")
        chunk = self.payload[self.offset:end]
        self.offset = end
        return chunk

    def read_byte(self) -> int:
        return self.read(1)[0]

    def read_uint16(self) -> int:
        return struct.unpack("!H", self.read(2))[0]

    def read_uint32(self) -> int:
        return struct.unpack("!I", self.read(4))[0]

    def read_int32(self) -> int:
        return struct.unpack("!i", self.read(4))[0]

    def read_uint64(self) -> int:
        return struct.unpack("!Q", self.read(8))[0]

    def read_int64(self) -> int:
        return struct.unpack("!q", self.read(8))[0]

    def read_cstring(self) -> str:
        try:
            end = self.payload.index(b"\x00", self.offset)
        except ValueError as exc:
            raise PgOutputDecodeError("Unterminated pgoutput cstring") from exc
        raw = self.payload[self.offset:end]
        self.offset = end + 1
        return raw.decode("utf-8")


def _decode_pg_timestamp(value: int) -> datetime:
    return _PG_EPOCH + timedelta(microseconds=value)


@dataclass(frozen=True)
class _PendingChange:
    source_op: str
    relation: PgOutputRelation
    old_key: dict[str, Any] | None
    row_after: dict[str, Any] | None
    change_index: int


class PgOutputDecoder:
    """Minimal native pgoutput decoder for INSERT/UPDATE/DELETE relation changes."""

    def __init__(self, source_table: str) -> None:
        self.source_table = source_table
        self.relations: dict[int, PgOutputRelation] = {}
        self.pending_changes: list[_PendingChange] = []
        self.current_xid: int | None = None
        self._change_index = 0

    def decode_message(self, payload: bytes) -> list[LogicalCdcSourceEvent]:
        reader = _Reader(payload)
        message_type = chr(reader.read_byte())

        if message_type == "B":
            return self._decode_begin(reader)
        if message_type == "C":
            return self._decode_commit(reader)
        if message_type == "R":
            self._decode_relation(reader)
            return []
        if message_type == "I":
            self._decode_insert(reader)
            return []
        if message_type == "U":
            self._decode_update(reader)
            return []
        if message_type == "D":
            self._decode_delete(reader)
            return []
        if message_type == "T":
            raise PgOutputDecodeError("TRUNCATE messages are not supported by logical_cdc strategy")

        return []

    def _decode_begin(self, reader: _Reader) -> list[LogicalCdcSourceEvent]:
        reader.read_uint64()  # final_lsn, commit message carries the applied commit LSN.
        reader.read_int64()  # commit timestamp; keep commit timestamp from COMMIT message.
        self.current_xid = reader.read_uint32()
        self.pending_changes = []
        self._change_index = 0
        return []

    def _decode_commit(self, reader: _Reader) -> list[LogicalCdcSourceEvent]:
        reader.read_byte()  # flags
        commit_lsn = int_to_lsn(reader.read_uint64())
        end_lsn = int_to_lsn(reader.read_uint64())
        commit_ts = _decode_pg_timestamp(reader.read_int64())
        events = [
            LogicalCdcSourceEvent(
                source_op=change.source_op,
                commit_lsn=commit_lsn,
                end_lsn=end_lsn,
                change_index=change.change_index,
                xid=self.current_xid,
                commit_ts=commit_ts,
                relation=change.relation,
                old_key=change.old_key,
                row_after=change.row_after,
            )
            for change in self.pending_changes
        ]
        self.pending_changes = []
        self.current_xid = None
        self._change_index = 0
        return events

    def _decode_relation(self, reader: _Reader) -> None:
        relation_id = reader.read_uint32()
        namespace = reader.read_cstring()
        relation_name = reader.read_cstring()
        replica_identity = chr(reader.read_byte())
        column_count = reader.read_uint16()
        columns: list[PgOutputRelationColumn] = []
        for _ in range(column_count):
            flags = reader.read_byte()
            name = reader.read_cstring()
            type_oid = reader.read_uint32()
            atttypmod = reader.read_int32()
            columns.append(
                PgOutputRelationColumn(
                    name=name,
                    type_oid=type_oid,
                    atttypmod=atttypmod,
                    flags=flags,
                )
            )
        self.relations[relation_id] = PgOutputRelation(
            relation_id=relation_id,
            schema=namespace,
            table=relation_name,
            replica_identity=replica_identity,
            columns=columns,
        )

    def _decode_insert(self, reader: _Reader) -> None:
        relation = self._read_relation(reader)
        tuple_kind = chr(reader.read_byte())
        if tuple_kind != "N":
            raise PgOutputDecodeError(f"Unexpected INSERT tuple kind: {tuple_kind}")
        row_after = self._read_tuple_data(reader, relation)
        self._append_change("I", relation, old_key=None, row_after=row_after)

    def _decode_update(self, reader: _Reader) -> None:
        relation = self._read_relation(reader)
        marker = chr(reader.read_byte())
        old_key = None
        if marker in {"K", "O"}:
            old_key = self._read_tuple_data(reader, relation)
            marker = chr(reader.read_byte())
        if marker != "N":
            raise PgOutputDecodeError(f"Unexpected UPDATE tuple kind: {marker}")
        row_after = self._read_tuple_data(reader, relation)
        self._append_change("U", relation, old_key=old_key, row_after=row_after)

    def _decode_delete(self, reader: _Reader) -> None:
        relation = self._read_relation(reader)
        tuple_kind = chr(reader.read_byte())
        if tuple_kind not in {"K", "O"}:
            raise PgOutputDecodeError(f"Unexpected DELETE tuple kind: {tuple_kind}")
        old_key = self._read_tuple_data(reader, relation)
        self._append_change("D", relation, old_key=old_key, row_after=None)

    def _read_relation(self, reader: _Reader) -> PgOutputRelation:
        relation_id = reader.read_uint32()
        relation = self.relations.get(relation_id)
        if relation is None:
            raise PgOutputDecodeError(f"Relation id {relation_id} was not announced before change message")
        return relation

    def _read_tuple_data(self, reader: _Reader, relation: PgOutputRelation) -> dict[str, Any]:
        column_count = reader.read_uint16()
        if column_count != len(relation.columns):
            raise PgOutputDecodeError(
                f"Tuple column count {column_count} does not match relation {relation.qualified_name}"
            )

        values: dict[str, Any] = {}
        for column in relation.columns:
            kind = chr(reader.read_byte())
            if kind == "n":
                values[column.name] = None
            elif kind == "u":
                values[column.name] = None
            elif kind == "t":
                value_length = reader.read_uint32()
                values[column.name] = reader.read(value_length).decode("utf-8")
            else:
                raise PgOutputDecodeError(f"Unsupported pgoutput tuple data kind: {kind}")
        return values

    def _append_change(
        self,
        source_op: str,
        relation: PgOutputRelation,
        old_key: dict[str, Any] | None,
        row_after: dict[str, Any] | None,
    ) -> None:
        if relation.qualified_name != self.source_table:
            return
        self.pending_changes.append(
            _PendingChange(
                source_op=source_op,
                relation=relation,
                old_key=old_key,
                row_after=row_after,
                change_index=self._change_index,
            )
        )
        self._change_index += 1
