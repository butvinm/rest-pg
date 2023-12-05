"""API errors."""


from fastapi import HTTPException, status


class TableNotFound(HTTPException):
    """Table not found error."""

    def __init__(self, table_name: str):
        """Init HTTPException."""
        super().__init__(
            status_code=status.HTTP_404_NOT_FOUND,
            detail='Table {table_name} doens`t exist'.format(
                table_name=table_name,
            ),
        )


class TableExists(HTTPException):
    """Table already exists error."""

    def __init__(self, table_name: str):
        """Init HTTPException."""
        super().__init__(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail='Table {table_name} already exists'.format(
                table_name=table_name,
            ),
        )


class PgError(HTTPException):
    """Database interaction error."""

    def __init__(self, error: str):
        """Init HTTPException."""
        super().__init__(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail='Bad SQL query: {err}'.format(err=error),
        )
