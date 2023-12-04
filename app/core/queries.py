"""SQL queries helpers."""


from psycopg.abc import Query
from psycopg.sql import SQL, Composed, Identifier, Placeholder
from pydantic import BaseModel

from app.core.models import TableDef

# Constraints
_NOT_NULL = 'NOT NULL'
_UNIQUE = 'UNIQUE'
_PRIMARY_KEY = 'PRIMARY KEY'

# Column definition template.
_COLUMN_DEF_QUERY = SQL('{name} {type} {constraints}')

# Template for create table query.
_TABLE_DEF_QUERY = SQL('CREATE TABLE {table_name} ({column_defs});')


def create_table_query(
    table_name: str,
    table_def: TableDef,
) -> Query:
    """Create SQL query for table declaration."""
    column_defs: list[Composed] = []
    for column in table_def.columns:
        constraints: list[str] = []
        if column.primary_key:
            constraints.append(_PRIMARY_KEY)
        else:
            if not column.nullable:
                constraints.append(_NOT_NULL)
            if column.unique:
                constraints.append(_UNIQUE)

        column_def = _COLUMN_DEF_QUERY.format(
            name=Identifier(column.name),
            type=SQL(column.type),
            constraints=SQL(' ').join(map(SQL, constraints)),
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


class TableSizeResult(BaseModel):
    """Result of table size query."""

    size: int


_TABLE_SIZE_QUERY = SQL("""
SELECT
    pg_total_relation_size(quote_ident(table_name)) as size
FROM
    information_schema.tables
WHERE table_name = {table_name};
""")


def table_size_query(table_name: str) -> Query:
    """Create query that get table size in bytes."""
    return _TABLE_SIZE_QUERY.format(table_name=table_name)


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


_INSERT_EMPTY_ROW_QUERY = SQL(
    'INSERT INTO {table_name} DEFAULT VALUES RETURNING *;')

_INSERT_ROW_QUERY = SQL("""
INSERT INTO {table_name}
({column_names})
VALUES ({placeholders})
RETURNING *;
""")


def insert_row_query(
    table_name: str,
    column_names: list[str],
) -> Query:
    """Create insert query."""
    if not column_names:
        return _INSERT_EMPTY_ROW_QUERY.format(
            table_name=Identifier(table_name),
        )

    return _INSERT_ROW_QUERY.format(
        table_name=Identifier(table_name),
        column_names=SQL(', ').join(map(Identifier, column_names)),
        placeholders=SQL(', ').join(Placeholder() * len(column_names)),
    )


_DROP_TABLE_QUERY = SQL('DROP TABLE {table_name}')


def drop_table_query(table_name: str) -> Query:
    """Create drop table query."""
    return _DROP_TABLE_QUERY.format(table_name=Identifier(table_name))
