"""Viadot exceptions."""


class ValidationError(Exception):
    pass


class APIError(Exception):
    pass


class CredentialError(Exception):
    pass


class DBDataAccessError(Exception):
    pass


class SAOStateError(Exception):
    """Base exception for invalid SAO state."""


class MissingStateEntryError(SAOStateError):
    """Raised when a state-tracked upstream has no state-table entry."""

    def __init__(self, upstream: str, downstream: str) -> None:
        """Build an error for a missing upstream state entry."""
        message = (
            f"State-tracked upstream '{upstream}' has no state-table entry while "
            f"evaluating downstream '{downstream}'."
        )
        super().__init__(message)


class DataRangeError(Exception):
    pass


class TableDoesNotExistError(Exception):
    def __init__(
        self,
        message: str | None = None,
        table: str | None = None,
        schema: str | None = None,
        fqn: str | None = None,
    ):
        """Raise when a database table does not exist.

        Args:
            table (str, optional): The name of the table. Defaults to None.
            schema (str, optional): The schema where the table is located. Defaults to
                None.
            fqn (str, optional): The fully-qualified name of the table. Defaults to
                None.
        """
        fqn = f"{schema}.{table}" if table and schema else fqn or table

        message = message or f"Table {fqn} does not exist."

        super().__init__(message)


class TableAlreadyExistsError(Exception):
    def __init__(
        self,
        message: str | None = None,
        table: str | None = None,
        schema: str | None = None,
        fqn: str | None = None,
    ):
        """Raise when a database table already exists.

        Args:
            table (str, optional): The name of the table. Defaults to None.
            schema (str, optional): The schema where the table is located. Defaults to
                None.
            fqn (str, optional): The fully-qualified name of the table. Defaults to
                None.
        """
        fqn = f"{schema}.{table}" if table and schema else fqn or table

        message = message or f"Table {fqn} already exists."

        super().__init__(message)


class DataBufferExceededError(Exception):
    pass


class NoFilesToProcessError(Exception):
    pass


class SMBError(Exception):
    pass


class SMBConnectionError(SMBError):
    pass


class SMBFileOperationError(SMBError):
    pass


class SMBInvalidFilenameError(SMBError):
    pass
