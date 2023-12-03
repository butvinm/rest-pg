"""Data models."""

from pydantic import BaseModel, Field


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


class ColumnInfo(BaseModel):
    """Column info."""

    # Column name.
    name: str

    # Column type: any of types, supported by Postgres.
    type: str


class TableInfo(BaseModel):
    """Table info."""

    # Table name.
    qualified_name: str

    # Columns data.
    columns: list[ColumnInfo]

    # Rows count
    rows: int = Field(ge=0)
