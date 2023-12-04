"""API dependencies providers."""

from typing import Any, AsyncGenerator

from psycopg import AsyncConnection

from app import config


async def db_connection() -> AsyncGenerator[AsyncConnection[Any], None]:
    """Provide asyncpg connection to Postgres database."""
    async with await AsyncConnection.connect(
        host=config.PG_HOST,
        port=config.PG_PORT,
        user=config.PG_USER,
        password=config.PG_PASSWORD,
        dbname=config.PG_DATABASE,
    ) as conn:
        yield conn
