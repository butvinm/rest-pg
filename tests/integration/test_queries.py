"""Queries builder tests."""


from typing import Any

import pytest
from psycopg import AsyncConnection
from psycopg.sql import Composed

from app.core.models import ColumnDef, ColumnTypes, TableDef
from app.core.queries import create_table_query, insert_row_query


@pytest.mark.parametrize(('table_name', 'table_def', 'expected'), (
    # Without constraints on last column.
    (
        'My Table',
        TableDef(
            columns=[
                ColumnDef(
                    name='col 1',
                    type=ColumnTypes.serial,
                    primary_key=True,
                ),
                ColumnDef(name='col 2', type=ColumnTypes.text),
            ],
        ),
        'CREATE TABLE "My Table" ("col 1" serial PRIMARY KEY, "col 2" text );',
    ),
    # Constraints
    (
        'Another Table',
        TableDef(
            columns=[
                ColumnDef(
                    name='id',
                    type=ColumnTypes.serial,
                    primary_key=True,
                ),
                ColumnDef(name='name', type=ColumnTypes.text, unique=True),
                ColumnDef(
                    name='age',
                    type=ColumnTypes.integer,
                    nullable=False,
                ),
            ],
        ),
        'CREATE TABLE "Another Table" ("id" serial PRIMARY KEY, "name" text UNIQUE, "age" integer NOT NULL);',
    ),
    # Primary key should exclude unique and not null.
    (
        'Yet Another Table',
        TableDef(
            columns=[
                ColumnDef(
                    name='id',
                    type=ColumnTypes.serial,
                    unique=True,
                    nullable=False,
                    primary_key=True,
                ),
            ],
        ),
        'CREATE TABLE "Yet Another Table" ("id" serial PRIMARY KEY);',
    ),
))
async def test_create_table_query(
    table_name: str,
    table_def: TableDef,
    expected: str,
    db_conn: AsyncConnection[Any],
) -> None:
    """Test `create_table_query` function."""
    query = create_table_query(table_name, table_def)
    assert isinstance(query, Composed)
    assert query.as_string(db_conn) == expected


@pytest.mark.parametrize(('table_name', 'column_names', 'expected'), (
    (
        'My Table',
        ['col 1', 'col 2'],
        'INSERT INTO "My Table"\n("col 1", "col 2")\nVALUES (%s, %s)\nRETURNING *;',
    ),
    (
        'My Table',
        [],
        'INSERT INTO "My Table" DEFAULT VALUES RETURNING *;',
    ),
))
async def test_insert_row_query(
    table_name: str,
    column_names: list[str],
    expected: str,
    db_conn: AsyncConnection[Any],
) -> None:
    """Test `insert_row_query` function."""
    query = insert_row_query(table_name, column_names)
    assert isinstance(query, Composed)
    assert query.as_string(db_conn).strip() == expected.strip()
