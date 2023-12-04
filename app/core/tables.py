"""Tables management operations."""


from typing import Any

from psycopg import AsyncConnection
from psycopg.errors import Error as PgError
from psycopg.rows import class_row, dict_row
from pydantic import BaseModel

from app.core.models import ColumnInfo, TableData, TableDef, TableInfo
from app.core.queries import (
    TableQualifiedNameResult,
    TableRowsResult,
    TableSizeResult,
    create_table_query,
    drop_table_query,
    insert_row_query,
    table_columns_query,
    table_qualified_name_query,
    table_rows_query,
    table_size_query,
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


async def _get_table_size(
    table_name: str,
    conn: AsyncConnection[Any],
) -> int | None | DbError:
    try:
        async with conn.cursor(
            row_factory=class_row(TableSizeResult),
        ) as curr:
            await curr.execute(table_size_query(table_name))
            record = await curr.fetchone()
    except PgError as err:
        return DbError(message=str(err))

    if record is None:
        return None

    return record.size


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

    size = await _get_table_size(table_name, conn)
    if size is None:
        return DbError(message='Failed to get rows count')
    elif isinstance(size, DbError):
        return size

    columns = await _get_table_columns(table_name, conn)
    if isinstance(columns, DbError):
        return columns

    return TableInfo(
        qualified_name=qualified_name,
        columns=columns,
        rows=rows,
        size=size,
    )


async def insert_rows(
    table_name: str,
    table_data: TableData,
    conn: AsyncConnection[Any],
) -> TableData | None | DbError:
    """Insert rows into table."""
    inserted = TableData(rows=[])
    if not table_data.rows:
        return inserted

    curr = conn.cursor(row_factory=dict_row)
    for row in table_data.rows:
        query = insert_row_query(table_name, column_names=list(row.keys()))
        try:
            async with conn.transaction():
                await curr.execute(query, tuple(row.values()))
                inserted_row = await curr.fetchone()
                if inserted_row is None:
                    return DbError(message='Failed to insert row')

                inserted.rows.append(inserted_row)
        except PgError as err:
            return DbError(message=str(err))

    return inserted


async def drop_table(
    table_name: str,
    conn: AsyncConnection[Any],
) -> str | None | DbError:
    """Drop table entry."""
    try:
        async with conn.transaction():
            await conn.execute(drop_table_query(table_name))
    except PgError as err:
        return DbError(message=str(err))

    return table_name
