"""SQL queries helpers."""


from app.core.models import TableDef

# Column definition template.
COLUMN_DEF_TPL = 'quote_ident({name}) {type}'
COLUMN_DEF_EMBELLISHED_TPL = 'quote_ident({name}) {type} {embellishment}'

# Template for create table query.
CREATE_TABLE_TPL = 'CREATE TABLE quote_ident({table_name}) ({column_defs});'


def create_table_query(
    table_name: str,
    table_def: TableDef,
) -> str:
    """Create SQL query for table declaration."""
    column_defs: list[str] = []
    for column_def in table_def.columns:
        if column_def.embellishment:
            template = COLUMN_DEF_EMBELLISHED_TPL
        else:
            template = COLUMN_DEF_TPL

        column_defs.append(
            template.format(**column_def.model_dump()),
        )

    return CREATE_TABLE_TPL.format(
        table_name=table_name,
        column_defs=', '.join(column_defs),
    )
