"""Queries builder tests."""


from typing import Any

from psycopg import AsyncConnection
from psycopg.sql import Composed

from app.core.models import ColumnDef, TableDef
from app.core.queries import create_table_query


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
