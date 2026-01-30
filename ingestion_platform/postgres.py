from __future__ import annotations

import re

from sqlalchemy import MetaData, Table, create_engine, inspect, text
from sqlalchemy.engine import Engine

_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def create_sqlalchemy_engine(dsn: str) -> Engine:
    return create_engine(dsn, pool_pre_ping=True)


def validate_identifier(name: str) -> str:
    if not _IDENTIFIER_RE.fullmatch(name):
        raise ValueError(f"Invalid SQL identifier: {name}")
    return name


def parse_table_name(table_name: str) -> tuple[str, str]:
    if "." not in table_name:
        schema, name = "public", table_name
    else:
        schema, name = table_name.split(".", 1)
    return validate_identifier(schema), validate_identifier(name)


def ensure_schema(engine: Engine, schema: str) -> None:
    safe_schema = validate_identifier(schema)
    with engine.begin() as conn:
        conn.execute(text(f'CREATE SCHEMA IF NOT EXISTS "{safe_schema}"'))


def table_exists(engine: Engine, schema: str, name: str) -> bool:
    safe_schema = validate_identifier(schema)
    safe_name = validate_identifier(name)
    inspector = inspect(engine)
    return inspector.has_table(safe_name, schema=safe_schema)


def reflect_table(engine: Engine, schema: str, name: str) -> Table:
    safe_schema = validate_identifier(schema)
    safe_name = validate_identifier(name)
    metadata = MetaData()
    return Table(safe_name, metadata, schema=safe_schema, autoload_with=engine)
