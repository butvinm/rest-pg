"""Data models."""

from enum import StrEnum
from typing import Any, Self

from pydantic import BaseModel, Field, model_validator


class ColumnTypes(StrEnum):
    """Supported columns types."""

    serial = 'serial'
    integer = 'integer'
    text = 'text'
    boolean = 'boolean'


class ColumnDef(BaseModel):
    """Data to define new table column."""

    # Column name.
    name: str = Field(min_length=1)

    # Column type: any of types, supported by Postgres.
    type: ColumnTypes

    # Set NULL constraint
    nullable: bool = True

    # Set UNIQUE constraint
    unique: bool = False

    # Set PRIMARY KEY constraint. Overrides `nullable` and `unique`
    primary_key: bool = False

    @model_validator(mode='after')
    def check_primary_key(self) -> Self:
        """Override `nullable` and `unique` for primary keys."""
        if self.primary_key:
            self.nullable = False
            self.unique = True

        return self


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

    # Totals size in bytes
    size: int = Field(ge=0)


class TableData(BaseModel):
    """Unstructured table data."""

    rows: list[dict[str, Any]]
