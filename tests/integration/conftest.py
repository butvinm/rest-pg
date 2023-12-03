"""Shared fixtures and resoruces."""

from asyncio import AbstractEventLoop, new_event_loop
from typing import Any, AsyncGenerator, Generator

import pytest
from psycopg import AsyncConnection
from testcontainers.postgres import PostgresContainer


@pytest.fixture(scope='session')
def event_loop() -> Generator[AbstractEventLoop, None, None]:
    """Create session-scoped shared event loop."""
    loop = new_event_loop()
    yield loop
    loop.close()


@pytest.fixture(scope='session')
async def db_conn() -> AsyncGenerator[AsyncConnection[Any], None]:
    """Create connection with Postgres in test container."""
    with PostgresContainer() as container:
        async with await AsyncConnection.connect(
            host=container.get_container_host_ip(),
            port=container.get_exposed_port(container.port_to_expose),
            user=container.POSTGRES_USER,
            password=container.POSTGRES_PASSWORD,
            dbname=container.POSTGRES_DB,
        ) as conn:
            yield conn
