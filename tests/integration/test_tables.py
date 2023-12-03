"""Tables manipulations tests."""


from typing import Any

from psycopg import AsyncConnection
from psycopg.rows import dict_row

from app.core.models import ColumnDef, TableDef
from app.core.tables import DbError, create_table


async def test_create_table(db_conn: AsyncConnection[Any]) -> None:
    """Test `create_table` function."""
    # Correct table
    result = await create_table(
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
        db_conn,
    )
    assert isinstance(result, str)

    async with db_conn.cursor(row_factory=dict_row) as curr:
        await curr.execute("SELECT * FROM pg_class WHERE relkind = 'r';")
        records = await curr.fetchmany()
        assert result in {record.get('relname') for record in records}

    # Bad column type
    result = await create_table(
        'Bad Table',
        TableDef(
            columns=[
                ColumnDef(
                    name='col 1',
                    type='UNKNOWN TYPE',
                ),
            ],
        ),
        db_conn,
    )
    assert isinstance(result, DbError)

    # Bad constrains
    result = await create_table(
        'Bad Table2',
        TableDef(
            columns=[
                ColumnDef(
                    name='col 1',
                    type='int',
                    embellishment='something',
                ),
            ],
        ),
        db_conn,
    )
    assert isinstance(result, DbError)
