"""PostgreSQL source connector."""

import logging

from pydantic import BaseModel, SecretStr
import pyodbc

from viadot.config import get_source_credentials
from viadot.sources.base import SQL


logger = logging.getLogger(__name__)


class PostgreSQLCredentials(BaseModel):
    user: str
    password: str | SecretStr | None = None
    server: str
    port: int = 5432
    db_name: str
    sslmode: str | None = None


class PostgreSQL(SQL):
    def __init__(
        self,
        credentials: PostgreSQLCredentials | None = None,
        config_key: str | None = None,
        driver: str = "PostgreSQL Unicode",
        query_timeout: int = 60,
        *args,
        **kwargs,
    ):
        """PostgreSQL connector.

        Args:
            driver (str | None, optional): ODBC driver. Default "PostgreSQL Unicode".
            query_timeout (int, optional): Query timeout in seconds. Defaults to 60.
        """
        raw_creds = credentials or get_source_credentials(config_key) or {}
        validated_creds = PostgreSQLCredentials(**raw_creds).dict(
            by_alias=True
        )  # validate the credentials

        super().__init__(
            *args,
            credentials=validated_creds,
            driver=driver,
            query_timeout=query_timeout,
            **kwargs,
        )
        self.server = self.credentials.get("server")
        self.port = self.credentials.get("port")
        self.db_name = self.credentials.get("db_name")
        self.user = self.credentials.get("user")
        self.password = self.credentials.get("password")
        self.sslmode = self.credentials.get("sslmode")

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
            f"SELECT 1 FROM information_schema.tables "  # noqa: S608
            f"WHERE table_schema = '{schema}' AND table_name = '{table}' LIMIT 1"
        )
        return bool(self.run(exists_query))

    @property
    def con(self) -> pyodbc.Connection:
        """Create a connection without forcing unsupported connection attributes.

        Some ODBC drivers (e.g., psqlODBC) do not support setting the connection
        timeout attribute via `self._con.timeout`. We ignore that if raised.

        Returns:
            pyodbc.Connection: The database connection.
        """
        if not self._con:
            # Keep a short login timeout to fail fast on bad hosts/ports
            self._con = __import__("pyodbc").connect(self.conn_str, timeout=5)
            try:
                # Best-effort; ignore if the driver doesn't support it
                self._con.timeout = self.query_timeout
            except Exception:
                logger.warning(
                    "The driver does not support setting the connection timeout attribute."
                )
        return self._con
