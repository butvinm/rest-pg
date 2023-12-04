"""Tables management operations."""


from typing import Any

from psycopg import AsyncConnection
from psycopg.errors import Error as PgError
from psycopg.rows import class_row
from pydantic import BaseModel

from app.core.models import ColumnInfo, TableDef, TableInfo
from app.core.queries import (
    TableQualifiedNameResult,
    TableRowsResult,
    table_columns_query,
    table_qualified_name_query,
    create_table_query,
    table_rows_query,
)


class DbError(BaseModel):
    """PostgresError representation."""

    message: str


async def create_table(
    table_name: str,
    table_def: TableDef,
    conn: AsyncConnection[Any],
) -> str | DbError:
    """Create new table in the database."""
    try:
        async with conn.cursor() as curr:
            await curr.execute(create_table_query(table_name, table_def))
    except PgError as err:
        return DbError(message=str(err))

    return table_name


async def _get_table_qualified_name(
    table_name: str,
    conn: AsyncConnection[Any],
) -> str | None | DbError:
    try:
        async with conn.cursor(
            row_factory=class_row(TableQualifiedNameResult),
        ) as curr:
            await curr.execute(table_qualified_name_query(table_name))
            record = await curr.fetchone()
    except PgError as err:
        return DbError(message=str(err))

    if record is None:
        return None

    return record.qualified_name


async def _get_table_rows(
    table_name: str,
    conn: AsyncConnection[Any],
) -> int | None | DbError:
    try:
        async with conn.cursor(
            row_factory=class_row(TableRowsResult),
        ) as curr:
            await curr.execute(table_rows_query(table_name))
            record = await curr.fetchone()
    except PgError as err:
        return DbError(message=str(err))

    if record is None:
        return None

    return record.rows


async def _get_table_columns(
    table_name: str,
    conn: AsyncConnection[Any],
) -> list[ColumnInfo] | DbError:
    try:
        async with conn.cursor(
            row_factory=class_row(ColumnInfo),
        ) as curr:
            await curr.execute(table_columns_query(table_name))
            return await curr.fetchall()
    except PgError as err:
        return DbError(message=str(err))


async def get_table_info(
    table_name: str,
    conn: AsyncConnection[Any],
) -> TableInfo | None | DbError:
    """Get table info from system catalog."""
    qualified_name = await _get_table_qualified_name(table_name, conn)
    if qualified_name is None or isinstance(qualified_name, DbError):
        return qualified_name

    rows = await _get_table_rows(table_name, conn)
    if rows is None:
        return DbError(message='Failed to get rows count')
    elif isinstance(rows, DbError):
        return rows

    columns = await _get_table_columns(table_name, conn)
    if isinstance(columns, DbError):
        return columns

    return TableInfo(
        qualified_name=qualified_name,
        columns=columns,
        rows=rows,
    )
