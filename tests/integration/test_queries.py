"""Queries builder tests."""


from typing import Any

from psycopg import AsyncConnection
from psycopg.sql import Composed

from app.core.models import ColumnDef, TableDef
from app.core.queries import create_table_query, insert_row_query


async def test_create_table_query(db_conn: AsyncConnection[Any]) -> None:
    """Test `create_table_query` function."""
    query = create_table_query(
        'My Table',
        TableDef(
            columns=[
                ColumnDef(
                    name='col 1',
                    type='serial',
                    embellishment='PRIMARY KEY',
                ),
                ColumnDef(name='col 2', type='text'),
            ],
        ),
    )
    expected = 'CREATE TABLE "My Table" ("col 1" serial PRIMARY KEY, "col 2" text);'
    assert isinstance(query, Composed)
    assert query.as_string(db_conn) == expected


async def test_insert_row_query(db_conn: AsyncConnection[Any]) -> None:
    """Test `insert_row_query` function."""
    query = insert_row_query('My Table', ['col 1', 'col 2'])
    expected = """
INSERT INTO "My Table"
("col 1", "col 2")
VALUES (%s, %s)
RETURNING *;
"""
    assert isinstance(query, Composed)
    assert query.as_string(db_conn) == expected

    query = insert_row_query('My Table', [])
    expected = 'INSERT INTO "My Table" DEFAULT VALUES RETURNING *;'
    assert isinstance(query, Composed)
    assert query.as_string(db_conn) == expected
