"""SQLite source connector."""

from viadot.sources.base import SQL


class SQLite(SQL):
    def __init__(
        self,
        query_timeout: int = 60,
        *args,
        **kwargs,
    ):
        """SQLite connector."""
        super().__init__(
            *args,
            driver="/usr/lib/x86_64-linux-gnu/odbc/libsqlite3odbc.so",
            query_timeout=query_timeout,
            **kwargs,
        )
        self.credentials["server"] = "localhost"

    @property
    def conn_str(self) -> str:
        """Generate a connection string from params or config.

        Note that the user and password are escaped with '{}' characters.

        Returns:
            str: The ODBC connection string.
        """
        driver = self.credentials["driver"]
        server = self.credentials["server"]
        db_name = self.credentials["db_name"]

        return f"DRIVER={{{driver}}};SERVER={server};DATABASE={db_name};"

    def _check_if_table_exists(self, table: str, schema: str | None = None) -> bool:
        """Checks if table exists.

        Args:
            table (str): Table name.
            schema (str, optional): Schema name. Defaults to None.
        """
        fqn = f"{schema}.{table}" if schema is not None else table
        exists_query = (
            f"SELECT name FROM sqlite_master WHERE type='table' AND name='{fqn}'"  # noqa: S608
        )
        return bool(self.run(exists_query))
