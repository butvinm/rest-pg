"""Tables management operations."""


from typing import Any

from psycopg import AsyncConnection
from psycopg.errors import Error as PgError
from pydantic import BaseModel

from app.core.models import Table, TableDef
from app.core.queries import create_table_query


class DbError(BaseModel):
    """PostgresError representation."""

    message: str


async def create_table(
    table_name: str,
    table_def: TableDef,
    conn: AsyncConnection[Any],
) -> str | DbError:
    """Create new table in the database."""
    query = create_table_query(table_name, table_def)
    async with conn.cursor() as curr:
        try:
            await curr.execute(query)
        except PgError as err:
            return DbError(message=str(err))

    return table_name

