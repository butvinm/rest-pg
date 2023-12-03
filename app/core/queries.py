"""SQL queries helpers."""


from psycopg.abc import Query
from psycopg.sql import SQL, Composed, Identifier

from app.core.models import TableDef

# Column definition template.
COLUMN_DEF = SQL('{name} {type}')
COLUMN_DEF_EMBELLISHED = SQL('{name} {type} {embellishment}')

# Template for create table query.
TABLE_DEF = SQL('CREATE TABLE {table_name} ({column_defs});')


def create_table_query(
    table_name: str,
    table_def: TableDef,
) -> Query:
    """Create SQL query for table declaration."""
    column_defs: list[Composed] = []
    for column in table_def.columns:
        if column.embellishment is not None:
            column_def = COLUMN_DEF_EMBELLISHED.format(
                name=Identifier(column.name),
                type=SQL(column.type),
                embellishment=SQL(column.embellishment),
            )
        else:
            column_def = COLUMN_DEF.format(
                name=Identifier(column.name),
                type=SQL(column.type),
            )
        column_defs.append(column_def)

    return TABLE_DEF.format(
        table_name=Identifier(table_name),
        column_defs=SQL(', ').join(column_defs),
    )
