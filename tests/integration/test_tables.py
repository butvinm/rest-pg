"""Tables manipulations tests."""


from typing import Any

from psycopg import AsyncConnection
from psycopg.rows import dict_row

from app.core.models import ColumnDef, TableDef, TableInfo
from app.core.tables import DbError, create_table, drop_table, get_table_info, insert_rows


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


async def test_insert_rows(db_conn: AsyncConnection[Any]) -> None:
    """Test `insert_rows` function."""
    # Create test table
    table_name = await create_table(
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
    assert isinstance(table_name, str)

    rows = await insert_rows(table_name, [], db_conn)
    assert rows == []

    rows = await insert_rows(
        table_name,
        [
            {'col 2': 'test 0'},
            {'col 2': 'test 1'},
        ],
        db_conn,
    )
    assert rows == [
        {'col 1': 1, 'col 2': 'test 0'},
        {'col 1': 2, 'col 2': 'test 1'},
    ]

    rows = await insert_rows(
        table_name,
        [
            {'col 1': 3, 'col 2': 'test 0'},
            {'col 1': 4, 'col 2': 'test 1'},
        ],
        db_conn,
    )
    assert rows == [
        {'col 1': 3, 'col 2': 'test 0'},
        {'col 1': 4, 'col 2': 'test 1'},
    ]


async def test_drop_table(db_conn: AsyncConnection[Any]) -> None:
    """Test `drop_table` function."""
    # Create test table
    table_name = await create_table(
        'My Table 3',
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
    assert isinstance(table_name, str)

    dropped_table = await drop_table(table_name, db_conn)

    assert isinstance(dropped_table, str)
    async with db_conn.cursor(row_factory=dict_row) as curr:
        await curr.execute("SELECT * FROM pg_class WHERE relkind = 'r';")
        records = await curr.fetchmany()
        assert dropped_table not in {record.get('relname') for record in records}

    dropped_table = await drop_table(table_name, db_conn)
    assert isinstance(dropped_table, DbError)


async def test_get_table_info(db_conn: AsyncConnection[Any]) -> None:
    """Test `get_table_info` function."""
    # Create test table
    table_name = await create_table(
        'My Table 2',
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
    assert isinstance(table_name, str)

    table_info = await get_table_info(table_name, db_conn)
    assert isinstance(table_info, TableInfo)

    assert table_info.qualified_name == 'test.public."My Table 2"'
    assert table_info.columns
    assert table_info.rows == 0
    assert table_info.size > 0
