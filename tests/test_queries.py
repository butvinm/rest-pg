"""Queries builder tests."""


from app.core.models import ColumnDef, TableDef
from app.core.queries import create_table_query


def test_create_table_query() -> None:
    """Test `create_table_query` function."""
    query = create_table_query(
        'My Table',
        TableDef(
            columns=[
                ColumnDef(
                    name='col 1',
                    type='serial',
                    embellishment='PRIMARY KEY',
                ),
                ColumnDef(name='col 2', type='text'),
            ],
        ),
    )
    expected = 'CREATE TABLE quote_ident(My Table) (quote_ident(col 1) serial PRIMARY KEY, quote_ident(col 2) text);'
    assert query == expected
