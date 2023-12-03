"""Tables management operations."""


from typing import Any

from psycopg import AsyncConnection
from psycopg.errors import Error as PgError
from pydantic import BaseModel

from app.core.models import Column, Table, TableDef
from app.core.queries import create_table_query


class DbError(BaseModel):
    """PostgresError representation."""

    message: str


async def create_table(
    table_name: str,
    table_def: TableDef,
    conn: AsyncConnection[Any],
) -> Table | DbError:
    """Create new table in the database."""
    query = create_table_query(table_name, table_def)
    try:
        await conn.execute(query)
    except PgError as err:
        return DbError(message=str(err))

    return Table(
        name=table_name,
        columns=[
            Column(**column_def.model_dump())
            for column_def in table_def.columns
        ],
    )
