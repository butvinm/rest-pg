"""Tables manipulations tests."""


from typing import Any, Type

import pytest
from psycopg import AsyncConnection
from psycopg.rows import dict_row

from app.core.models import (
    ColumnDef,
    ColumnTypes,
    TableData,
    TableDef,
    TableInfo,
)
from app.core.tables import (
    DbError,
    create_table,
    drop_table,
    get_table_info,
    insert_rows,
)
from tests.integration.conftest import TEST_TABLE_INFO, TEST_TABLE_NAME


@pytest.mark.parametrize(('table_name', 'table_def', 'expected_type', 'should_exists'), (
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
        str,
        True,
    ),
    (
        'Bad Table',
        TableDef(
            columns=[
                ColumnDef.model_construct(name='col 1', type='UnknownType'),
            ],
        ),
        DbError,
        False,
    ),
))
async def test_create_table(
    table_name: str,
    table_def: TableDef,
    expected_type: Type[Any],
    should_exists: bool,
    db_conn: AsyncConnection[Any],
) -> None:
    """Test `create_table` function."""
    created = await create_table(table_name, table_def, db_conn)
    assert isinstance(created, expected_type)

    async with db_conn.cursor(row_factory=dict_row) as curr:
        await curr.execute("SELECT * FROM pg_class WHERE relkind = 'r';")
        records = await curr.fetchmany()
        exists = str(created) in {record.get('relname') for record in records}
        assert exists == should_exists


@pytest.mark.parametrize(('table_data', 'expected'), (
    (
        TableData(rows=[
            {'col 1': 3, 'col 2': 'test 0'},
            {'col 1': 4, 'col 2': 'test 1'},
        ]),
        TableData(rows=[
            {'col 1': 3, 'col 2': 'test 0'},
            {'col 1': 4, 'col 2': 'test 1'},
        ]),
    ),
    (
        TableData(rows=[]),
        TableData(rows=[]),
    ),
    # Default values in id
    (
        TableData(rows=[
            {'col 2': 'test 0'},
            {'col 2': 'test 1'},
        ]),
        TableData(rows=[
            {'col 1': 1, 'col 2': 'test 0'},
            {'col 1': 2, 'col 2': 'test 1'},
        ]),
    ),
))
async def test_insert_rows(
    table_data: TableData,
    expected: TableData | None | DbError,
    empty_table: str,
    db_conn: AsyncConnection[Any],
) -> None:
    """Test `insert_rows` function."""
    inserted = await insert_rows(empty_table, table_data, db_conn)
    assert inserted == expected


@pytest.mark.parametrize(('table_name', 'expected_type'), (
    (TEST_TABLE_NAME, str),
    ('Unexisted', DbError),
))
async def test_drop_table(
    table_name: str,
    expected_type: Type[Any],
    empty_table: str,
    db_conn: AsyncConnection[Any],
) -> None:
    """Test `drop_table` function."""
    dropped = await drop_table(table_name, db_conn)

    assert isinstance(dropped, expected_type)
    async with db_conn.cursor(row_factory=dict_row) as curr:
        await curr.execute("SELECT * FROM pg_class WHERE relkind = 'r';")
        records = await curr.fetchmany()
        assert str(dropped) not in {
            record.get('relname') for record in records
        }


@pytest.mark.parametrize(('table_name', 'expected'), (
    (TEST_TABLE_NAME, TEST_TABLE_INFO),
    ('Unexisted', None),
))
async def test_get_table_info(
    table_name: str,
    expected: TableInfo,
    empty_table: str,
    db_conn: AsyncConnection[Any],
) -> None:
    """Test `get_table_info` function."""
    table_info = await get_table_info(table_name, db_conn)
    assert table_info == expected
