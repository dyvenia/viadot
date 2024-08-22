"""SQL Server source class."""

from datetime import datetime, timedelta, timezone
import struct

from pydantic import BaseModel, SecretStr

from viadot.config import get_source_credentials
from viadot.sources.base import SQL


class SQLServerCredentials(BaseModel):
    user: str
    password: str | SecretStr | None = None
    server: str
    driver: str = "ODBC Driver 17 for SQL Server"
    db_name: str | None = None


class SQLServer(SQL):
    DEFAULT_SCHEMA = "dbo"

    def __init__(
        self,
        credentials: SQLServerCredentials | None = None,
        config_key: str | None = None,
        *args,
        **kwargs,
    ):
        """Connector for SQL Server.

        Args:
            credentials (SQLServerCredentials | None, optional): The credentials to use.
                Defaults to None.
            config_key (str | None, optional): The viadot config key from which to read
                the credentials. Defaults to None.
        """
        raw_creds = credentials or get_source_credentials(config_key) or {}
        validated_creds = SQLServerCredentials(**raw_creds).dict(
            by_alias=True
        )  # validate the credentials

        super().__init__(*args, credentials=validated_creds, **kwargs)
        self.server = self.credentials.get("server")
        self.username = self.credentials.get("username")
        self.password = self.credentials.get("password")
        self.driver = self.credentials.get("driver")
        self.db_name = self.credentials.get("db_name")

        self.con.add_output_converter(-155, self._handle_datetimeoffset)

    @property
    def schemas(self) -> list[str]:
        """Return a list of all schemas."""
        schemas_tuples = self.run("SELECT s.name as schema_name from sys.schemas s")
        return [schema_tuple[0] for schema_tuple in schemas_tuples]

    @property
    def tables(self) -> list[str]:
        """Return a list of all tables in the database."""
        tables_tuples = self.run(
            "SELECT schema_name(t.schema_id), t.name FROM sys.tables t"
        )
        return [".".join(row) for row in tables_tuples]

    @staticmethod
    def _handle_datetimeoffset(dto_value: str) -> datetime:
        """Adds support for SQL Server's custom `datetimeoffset` type.

        This type is not handled natively by ODBC/pyodbc.

        See: https://github.com/mkleehammer/pyodbc/issues/134#issuecomment-281739794
        """
        (
            year,
            month,
            day,
            hour,
            minute,
            second,
            nanoseconds,
            offset_hours,
            offset_minutes,
        ) = struct.unpack("<6hI2h", dto_value)
        return datetime(
            year,
            month,
            day,
            hour,
            minute,
            second,
            nanoseconds // 1000,
            tzinfo=timezone(timedelta(hours=offset_hours, minutes=offset_minutes)),
        )

    def exists(self, table: str, schema: str | None = None) -> bool:
        """Check whether a table exists.

        Args:
            table (str): The table to be checked.
            schema (str, optional): The schema where the table is located.
                Defaults to 'dbo'.

        Returns:
            bool: Whether the table exists.
        """
        if not schema:
            schema = self.DEFAULT_SCHEMA

        list_table_info_query = f"""
            SELECT *
            FROM sys.tables t
            JOIN sys.schemas s
                ON t.schema_id = s.schema_id
            WHERE s.name = '{schema}' AND t.name = '{table}'
        """  # noqa: S608
        return bool(self.run(list_table_info_query))
