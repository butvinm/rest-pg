"""Data models."""

from pydantic import BaseModel


class ColumnDef(BaseModel):
    """Data to define new table column."""

    # Column name.
    name: str

    # Column type: any of types, supported by Postgres.
    type: str

    # Checks, constrains and other stuff that follow after type.
    embellishment: str | None = None


class TableDef(BaseModel):
    """Data to define new table."""

    # Columns data.
    columns: list[ColumnDef]


class Column(BaseModel):
    """Full column data."""

    # Column name.
    name: str

    # Column type: any of types, supported by Postgres.
    type: str

    # Checks, constrains and other stuff that follow after type.
    embellishment: str | None = None


class Table(BaseModel):
    """Full table data."""

    # Table name.
    name: str

    # Columns data.
    columns: list[Column]
