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
    """Raised when tracked upstream states are missing during downstream evaluation."""

    def __init__(
        self,
        upstream: str,
        downstream: str,
        *,
        missing_entries: dict[str, list[str]] | None = None,
        runnable_nodes: list[str] | None = None,
        stale_nodes: dict[str, list[str]] | None = None,
        missing_deployments: list[str] | None = None,
    ) -> None:
        """Build an error, optionally carrying all downstream evaluation results."""
        self.upstream = upstream
        self.downstream = downstream
        self.missing_entries = missing_entries or {downstream: [upstream]}
        self.runnable_nodes = runnable_nodes or []
        self.stale_nodes = stale_nodes or {}
        self.missing_deployments = missing_deployments or []

        details = "; ".join(
            f"{node}: {', '.join(upstreams)}"
            for node, upstreams in self.missing_entries.items()
        )
        message = (
            f"State-tracked upstream '{upstream}' has no state-table entry while "
            f"evaluating downstream '{downstream}'. "
            f"Missing state entries for tracked upstreams: {details}."
        )
        if self.missing_deployments:
            message += (
                " Missing downstream deployments: "
                f"{', '.join(self.missing_deployments)}."
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
