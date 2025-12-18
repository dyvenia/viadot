"""PostgreSQL source connector."""

import platform

from viadot.sources.base import SQL


class PostgreSQL(SQL):
    def __init__(
        self,
        driver: str | None = None,
        query_timeout: int = 60,
        *args,
        **kwargs,
    ):
        """PostgreSQL connector.

        Args:
            driver (str | None, optional): ODBC driver name or path. If not provided,
                defaults per OS:
                - Windows: 'PostgreSQL Unicode(x64)'
                - Linux/WSL: 'PostgreSQL Unicode'
                - macOS: 'PostgreSQL Unicode'
            query_timeout (int, optional): Query timeout in seconds. Defaults to 60.
        """
        if driver is None:
            system_name = platform.system().lower()
            if system_name == "windows":
                resolved_driver = "PostgreSQL Unicode(x64)"
            else:
                # Linux, WSL, macOS typically register the driver as 'PostgreSQL Unicode'
                resolved_driver = "PostgreSQL Unicode"
        else:
            resolved_driver = driver

        super().__init__(
            *args,
            driver=resolved_driver,
            query_timeout=query_timeout,
            **kwargs,
        )
        # Set sensible defaults if not provided
        self.credentials.setdefault("server", "localhost")
        self.credentials.setdefault("port", 5432)

    @property
    def conn_str(self) -> str:
        """Generate a PostgreSQL ODBC connection string from credentials.

        Returns:
            str: The ODBC connection string.
        """
        driver = self.credentials["driver"]
        server = self.credentials["server"]
        port = self.credentials.get("port", 5432)
        db_name = self.credentials["db_name"]
        uid = self.credentials.get("user") or ""
        pwd = self.credentials.get("password") or ""

        conn_str = (
            f"DRIVER={{{driver}}};"
            f"SERVER={server};"
            f"PORT={port};"
            f"DATABASE={db_name};"
            f"UID={uid};"
            f"PWD={pwd};"
        )

        # Optional SSL mode if provided, e.g. 'require', 'verify-ca', 'disable'
        if "sslmode" in self.credentials:
            conn_str += f"SSLmode={self.credentials['sslmode']};"

        return conn_str

    def _check_if_table_exists(self, table: str, schema: str | None = None) -> bool:
        """Check if a table exists in the given schema (PostgreSQL).

        Args:
            table (str): Table name.
            schema (str, optional): Schema name. Defaults to 'public' if not provided.
        """
        schema = schema or "public"
        exists_query = (
            f"SELECT 1 FROM information_schema.tables "
            f"WHERE table_schema = '{schema}' AND table_name = '{table}' LIMIT 1"  # noqa: S608
        )
        return bool(self.run(exists_query))


    def is_connected(self) -> bool:
        """Return True if a connection has been established and is healthy.

        This checks whether an underlying connection exists and verifies it
        by executing a lightweight `SELECT 1` on the existing connection.
        It will NOT create a new connection if none exists.
        """
        if self._con is None:
            return False
        try:
            cursor = self._con.cursor()
            cursor.execute("SELECT 1")
            cursor.fetchone()
            cursor.close()
            return True
        except Exception:
            return False

