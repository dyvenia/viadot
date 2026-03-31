"""Informix (Informix JDBC) source class."""

from typing import Any, Literal


try:
    import jaydebeapi
except ModuleNotFoundError as e:
    msg = "Missing required modules to use informix source."
    raise ImportError(msg) from e

import pandas as pd
from pydantic import BaseModel, SecretStr

from viadot.config import get_source_credentials
from viadot.sources.base import SQL, Record
from viadot.utils import add_viadot_metadata_columns, validate


class InformixCredentials(BaseModel):
    user: str
    password: str | SecretStr | None = None
    server: str
    port: int = 1504
    db_name: str
    informix_server: str
    db_locale: str = "en_US.57372"
    client_locale: str = "en_US.819"
    driver_class: str = "com.informix.jdbc.IfxDriver"


class Informix(SQL):
    DEFAULT_SCHEMA = "informix"

    def __init__(
        self,
        credentials: InformixCredentials | None = None,
        config_key: str | None = None,
        *args,
        **kwargs,
    ):
        """Connector for Informix Informix over JDBC."""
        raw_creds = credentials or get_source_credentials(config_key) or {}
        validated_creds = InformixCredentials(**raw_creds).dict(by_alias=True)

        super().__init__(*args, credentials=validated_creds, **kwargs)

        self.server = self.credentials.get("server")
        self.port = self.credentials.get("port")
        self.db_name = self.credentials.get("db_name")
        self.informix_server = self.credentials.get("informix_server")
        self.db_locale = self.credentials.get("db_locale")
        self.client_locale = self.credentials.get("client_locale")
        self.driver_class = self.credentials.get("driver_class")
        self.user = self.credentials.get("user")
        self.password = self.credentials.get("password")

    @property
    def conn_str(self) -> str:
        """Generate a JDBC URL for Informix.

        Returns:
            str: The JDBC URL.
        """
        return (
            f"jdbc:informix-sqli://{self.server}:{self.port}/{self.db_name}:"
            f"INFORMIXSERVER={self.informix_server};"
            f"DB_LOCALE={self.db_locale};"
            f"CLIENT_LOCALE={self.client_locale};"
        )

    @property
    def con(self) -> jaydebeapi.Connection:
        """Create and cache a JDBC connection.

        Returns:
            jaydebeapi.Connection: The JDBC connection.
        """
        if not self._con:
            password = (
                self.password.get_secret_value()
                if isinstance(self.password, SecretStr)
                else self.password
            )
            connect_args = [
                self.driver_class,
                self.conn_str,
                [self.user, password or ""],
            ]
            self._con = jaydebeapi.connect(*connect_args)
        return self._con

    @property
    def owners(self) -> list[str]:
        """Return all owner names.

        Returns:
            list[str]: The list of owners.
        """
        owners_tuples = self.run(
            "SELECT DISTINCT owner FROM systables WHERE owner IS NOT NULL"
        )
        return [owner_tuple[0] for owner_tuple in owners_tuples]

    @property
    def tables(self) -> list[str]:
        """Return all user tables.

        Returns:
            list[str]: The list of tables.
        """
        tables_tuples = self.run(
            "SELECT owner, tabname FROM systables WHERE tabtype = 'T' AND owner IS NOT NULL"
        )
        return [".".join(row) for row in tables_tuples]

    def exists(self, table: str, owner: str | None = None) -> bool:
        """Check whether a table exists.

        Args:
            table (str): The table to check.
            owner (str): The owner to check.

        Returns:
            bool: True if the table exists, False otherwise.
        """
        owner = owner or self.DEFAULT_OWNER
        list_table_info_query = f"""
            SELECT 1
            FROM systables
            WHERE owner = '{owner}' AND tabname = '{table}'
        """  # noqa: S608
        return bool(self.run(list_table_info_query))

    def _check_if_table_exists(self, table: str, owner: str | None = None) -> bool:
        """Check if table exists in a specified owner.

        Args:
            table (str): The table to check.
            owner (str): The owner to check.

        Returns:
            bool: True if the table exists, False otherwise.
        """
        return self.exists(table=table, owner=owner)

    def run(self, query: str) -> list[Record] | bool:
        """Execute a query and return query results.

        Args:
            query (str): The query to execute.

        Returns:
            list[Record] | bool: The result of the query.
        """
        cursor = self.con.cursor()
        cursor.execute(query)

        query_sanitized = query.strip().upper()
        if query_sanitized.startswith("SELECT") or query_sanitized.startswith("WITH"):
            result = cursor.fetchall()
        else:
            result = True

        self.con.commit()
        cursor.close()
        return result

    @add_viadot_metadata_columns
    def to_df(
        self,
        query: str,
        con: Any | None = None,  # noqa: ANN401
        if_empty: Literal["warn", "skip", "fail"] = "warn",
        tests: dict[str, Any] | None = None,
    ) -> pd.DataFrame:
        """Execute a query and return the result as a pandas DataFrame.

        Args:
            query (str): The query to execute.
            con (Any): The connection to use to pull the data.
            if_empty (Literal["warn", "skip", "fail"]):
                What to do if the query returns no data.
            tests (dict[str, Any]): The tests to run on the data.

        Returns:
            pd.DataFrame: The result of the query.
        """
        conn = con or self.con
        query_sanitized = query.strip().upper()

        if query_sanitized.startswith("SELECT") or query_sanitized.startswith("WITH"):
            with conn.cursor() as cursor:
                cursor.execute(query)
                rows = cursor.fetchall()
                cols = (
                    [description[0] for description in cursor.description]
                    if cursor.description
                    else []
                )
            df = pd.DataFrame(rows, columns=cols)
            if df.empty:
                self._handle_if_empty(if_empty=if_empty)
        else:
            self.run(query)
            df = pd.DataFrame()

        if tests:
            validate(df=df, tests=tests)

        return df
