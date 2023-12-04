"""SQL queries helpers."""


from psycopg.abc import Query
from psycopg.sql import SQL, Composed, Identifier
from pydantic import BaseModel

from app.core.models import TableDef

# Column definition template.
_COLUMN_DEF_QUERY = SQL('{name} {type}')
_COLUMN_DEF_EMBELLISHED_QUERY = SQL('{name} {type} {embellishment}')

# Template for create table query.
_TABLE_DEF_QUERY = SQL('CREATE TABLE {table_name} ({column_defs});')


def create_table_query(
    table_name: str,
    table_def: TableDef,
) -> Query:
    """Create SQL query for table declaration."""
    column_defs: list[Composed] = []
    for column in table_def.columns:
        if column.embellishment is not None:
            column_def = _COLUMN_DEF_EMBELLISHED_QUERY.format(
                name=Identifier(column.name),
                type=SQL(column.type),
                embellishment=SQL(column.embellishment),
            )
        else:
            column_def = _COLUMN_DEF_QUERY.format(
                name=Identifier(column.name),
                type=SQL(column.type),
            )
        column_defs.append(column_def)

    return _TABLE_DEF_QUERY.format(
        table_name=Identifier(table_name),
        column_defs=SQL(', ').join(column_defs),
    )


class TableQualifiedNameResult(BaseModel):
    """Result of table qualified name query."""

    qualified_name: str


_TABLE_QUALIFIED_NAME_QUERY = SQL("""
SELECT
    quote_ident(table_catalog)
    || '.'
    || quote_ident(table_schema)
    || '.'
    || quote_ident(table_name)
    as qualified_name
FROM
    information_schema.tables
WHERE
    table_name = {table_name}
    AND table_type = 'BASE TABLE';
""")


def table_qualified_name_query(table_name: str) -> Query:
    """Create query that full qualified name of table."""
    return _TABLE_QUALIFIED_NAME_QUERY.format(table_name=table_name)


class TableRowsResult(BaseModel):
    """Result of table rows query."""

    rows: int


_TABLE_ROWS_QUERY = SQL('SELECT count(*) as rows FROM {table_name}')


def table_rows_query(table_name: str) -> Query:
    """Create query that get table rows count."""
    return _TABLE_ROWS_QUERY.format(table_name=Identifier(table_name))


_TABLE_COLUMNS_QUERY = SQL("""
SELECT
    column_name as name, data_type as type
FROM
    information_schema.columns
WHERE
    table_name = {table_name};
""")


def table_columns_query(table_name: str) -> Query:
    """Create query that get table columns info."""
    return _TABLE_COLUMNS_QUERY.format(table_name=table_name)
