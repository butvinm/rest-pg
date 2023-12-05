"""Tables management operations."""


from typing import Any

from psycopg import AsyncConnection
from psycopg.errors import Error as PgError
from psycopg.rows import class_row, dict_row
from pydantic import BaseModel

from app.core.models import ColumnInfo, TableData, TableDef, TableInfo
from app.core.queries import (
    TableInfoResult,
    create_table_query,
    drop_table_query,
    insert_row_query,
    table_columns_query,
    table_exist_query,
    table_info_query,
)


class DbError(BaseModel):
    """PostgresError representation."""

    message: str


async def is_table_exist(
    table_name: str,
    conn: AsyncConnection[Any],
) -> bool | DbError:
    """Check that table exists in the database."""
    try:
        async with conn.transaction():
            curr = await conn.execute(table_exist_query(table_name))
            result: tuple[bool] | None = await curr.fetchone()
            if result is None:
                return False

            return result[0]
    except PgError as err:
        return DbError(message=str(err))


async def create_table(
    table_name: str,
    table_def: TableDef,
    conn: AsyncConnection[Any],
) -> str | None | DbError:
    """Create new table in the database."""
    table_exists = await is_table_exist(table_name, conn)
    if table_exists:
        return None
    elif isinstance(table_exists, DbError):
        return table_exists
    try:
        async with conn.transaction():
            await conn.execute(create_table_query(table_name, table_def))
    except PgError as err:
        return DbError(message=str(err))

    return table_name


async def _get_table_info(
    table_name: str,
    conn: AsyncConnection[Any],
) -> TableInfoResult | None | DbError:
    table_exists = await is_table_exist(table_name, conn)
    if not table_exists:
        return None
    elif isinstance(table_exists, DbError):
        return table_exists

    curr = conn.cursor(row_factory=class_row(TableInfoResult))
    try:
        async with conn.transaction():
            await curr.execute(table_info_query(table_name))
            table_info_result = await curr.fetchone()
    except PgError as err:
        return DbError(message=str(err))

    return table_info_result


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
    table_info = await _get_table_info(table_name, conn)
    if table_info is None or isinstance(table_info, DbError):
        return table_info

    columns = await _get_table_columns(table_name, conn)
    if isinstance(columns, DbError):
        return columns

    return TableInfo(
        qualified_name=table_info.qualified_name,
        columns=columns,
        rows=table_info.rows,
        size=table_info.size,
    )


async def insert_rows(
    table_name: str,
    table_data: TableData,
    conn: AsyncConnection[Any],
) -> TableData | None | DbError:
    """Insert rows into table."""
    table_exists = await is_table_exist(table_name, conn)
    if not table_exists:
        return None
    elif isinstance(table_exists, DbError):
        return table_exists

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
    table_exists = await is_table_exist(table_name, conn)
    if not table_exists:
        return None
    elif isinstance(table_exists, DbError):
        return table_exists

    try:
        async with conn.transaction():
            await conn.execute(drop_table_query(table_name))
    except PgError as err:
        return DbError(message=str(err))

    return table_name
