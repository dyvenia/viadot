class ValidationError(Exception):
    pass


class APIError(Exception):
    pass


class CredentialError(Exception):
    pass


class DBDataAccessError(Exception):
    pass


class DataRangeError(Exception):
    pass


class TableDoesNotExist(Exception):
    def __init__(
        self,
        message: str = None,
        table: str = None,
        schema: str = None,
        fqn: str = None,
    ):
        """
        Raise when a database table does not exist.

        Args:
            table (str, optional): The name of the table. Defaults to None.
            schema (str, optional): The schema where the table is located. Defaults to None.
            fqn (str, optional): The fully-qualified name of the table. Defaults to None.
        """
        if table and schema:
            fqn = f"{schema}.{table}"
        else:
            fqn = fqn or table

        message = message or f"Table {fqn} does not exist."

        super().__init__(message)


class TableAlreadyExists(Exception):
    def __init__(
        self,
        message: str = None,
        table: str = None,
        schema: str = None,
        fqn: str = None,
    ):
        """
        Raise when a database table already exists.

        Args:
            table (str, optional): The name of the table. Defaults to None.
            schema (str, optional): The schema where the table is located. Defaults to None.
            fqn (str, optional): The fully-qualified name of the table. Defaults to None.
        """
        if table and schema:
            fqn = f"{schema}.{table}"
        else:
            fqn = fqn or table

        message = message or f"Table {fqn} already exists."

        super().__init__(message)


class DataBufferExceeded(Exception):
    pass
