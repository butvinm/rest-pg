"""Shared fixtures and resoruces."""

from asyncio import AbstractEventLoop, new_event_loop
from typing import Any, AsyncGenerator, Generator

import pytest
from psycopg import AsyncConnection
from testcontainers.postgres import PostgresContainer

from app.core.models import (
    ColumnDef,
    ColumnInfo,
    ColumnTypes,
    TableDef,
    TableInfo,
)
from app.core.tables import create_table, drop_table


@pytest.fixture(scope='session')
def event_loop() -> Generator[AbstractEventLoop, None, None]:
    """Create session-scoped shared event loop."""
    loop = new_event_loop()
    yield loop
    loop.close()


@pytest.fixture(scope='session')
async def container() -> AsyncGenerator[PostgresContainer, None]:
    """Create connection with Postgres in test container."""
    with PostgresContainer() as container:
        yield container


@pytest.fixture(scope='session')
async def db_conn(
    container: PostgresContainer,
) -> AsyncGenerator[AsyncConnection[Any], None]:
    """Create connection with Postgres in test container."""
    async with await AsyncConnection.connect(
        host=container.get_container_host_ip(),
        port=container.get_exposed_port(container.port_to_expose),
        user=container.POSTGRES_USER,
        password=container.POSTGRES_PASSWORD,
        dbname=container.POSTGRES_DB,
    ) as conn:
        yield conn


@pytest.fixture(autouse=True)
async def drop_tables(db_conn: AsyncConnection[Any]) -> AsyncGenerator[None, None]:
    """Remove all tables after each test."""
    yield
    await db_conn.execute('drop schema public cascade; create schema public;')


TEST_TABLE_NAME = 'Test Table'
TEST_TABLE_DEF = TableDef(
    columns=[
        ColumnDef(
            name='col 1',
            type=ColumnTypes.serial,
            primary_key=True,
        ),
        ColumnDef(name='col 2', type=ColumnTypes.text),
    ],
)
TEST_TABLE_INFO = TableInfo(
    qualified_name='test.public."Test Table"',
    columns=[
        ColumnInfo(name='col 1', type='integer'),
        ColumnInfo(name='col 2', type='text'),
    ],
    rows=0,
    size=16384,  # Temporary solution, should determine proper size
)


@pytest.fixture
async def empty_table(
    db_conn: AsyncConnection[Any],
) -> AsyncGenerator[str, None]:
    """Create one-time table."""
    table = await create_table(TEST_TABLE_NAME, TEST_TABLE_DEF, db_conn)
    assert isinstance(table, str)
    yield table
    await drop_table(table, db_conn)
