"""Tables API endpoints."""

from typing import Annotated, Any, TypeAlias

from fastapi import Depends, status
from fastapi.routing import APIRouter
from psycopg import AsyncConnection

from app.api_v1.dependencies import db_connection
from app.api_v1.errors import PgError, TableNotFound
from app.core.models import TableData, TableDef, TableInfo
from app.core.tables import (
    DbError,
    create_table,
    drop_table,
    get_table_info,
    insert_rows,
)

router = APIRouter(prefix='/tables')

ConnectionDep: TypeAlias = Annotated[
    AsyncConnection[Any],
    Depends(db_connection),
]


@router.post(
    '/{table_name}',
    status_code=status.HTTP_201_CREATED,
)
async def create_table_handler(
    table_name: str,
    table_def: TableDef,
    conn: ConnectionDep,
) -> str:
    """Create new table in the database."""
    created = await create_table(table_name, table_def, conn)
    if isinstance(created, DbError):
        raise PgError(created.message)

    return created


@router.put(
    '/{table_name}',
    status_code=status.HTTP_200_OK,
)
async def insert_rows_handler(
    table_name: str,
    table_data: TableData,
    conn: ConnectionDep,
) -> TableData:
    """Insert new rows into table."""
    inserted = await insert_rows(table_name, table_data, conn)
    if inserted is None:
        raise TableNotFound(table_name)
    elif isinstance(inserted, DbError):
        raise PgError(inserted.message)

    return inserted


@router.delete(
    '/{table_name}',
    status_code=status.HTTP_200_OK,
)
async def drop_table_handler(
    table_name: str,
    conn: ConnectionDep,
) -> str:
    """Remove table from database."""
    dropped = await drop_table(table_name, conn)
    if dropped is None:
        raise TableNotFound(table_name)
    elif isinstance(dropped, DbError):
        raise PgError(dropped.message)

    return dropped


@router.get(
    '/table_info/{table_name}',
    status_code=status.HTTP_200_OK,
)
async def table_info_handler(
    table_name: str,
    conn: ConnectionDep,
) -> TableInfo:
    """Get table metainfo."""
    table_info = await get_table_info(table_name, conn)
    if table_info is None:
        raise TableNotFound(table_name)
    elif isinstance(table_info, DbError):
        raise PgError(table_info.message)

    return table_info
