"""Base classes for data sources."""

from abc import abstractmethod
import logging
from pathlib import Path
from typing import Any, Literal

import pandas as pd
import pyarrow as pa
import pyodbc

from viadot.config import get_source_credentials
from viadot.signals import SKIP


logger = logging.getLogger(__name__)

Record = tuple[Any]


class Source:
    def __init__(self, *args, credentials: dict[str, Any] | None = None, **kwargs):  # noqa: ARG002
        """Base class for data sources.

        Args:
            credentials (dict[str, Any] | None, optional): The credentials for the
                source. Defaults to None.
        """
        self.credentials = credentials
        self.data: pa.Table = None
        self.logger = logger

    @abstractmethod
    def to_json(self) -> dict:
        """Download data from source to a dictionary."""

    @abstractmethod
    def to_df(self, if_empty: Literal["warn", "skip", "fail"] = "warn") -> pd.DataFrame:
        """Download data from source to a pandas DataFrame.

        Args:
            if_empty (Literal[warn, skip, fail], optional): What to do if there is no
                data. Defaults to "warn".

        Returns:
            pd.DataFrame: The data from the source as a pandas DataFrame.
        """

    @abstractmethod
    def query(self) -> list[Record] | bool:
        """Run a query and possibly return the results."""
        pass

    def to_arrow(self, if_empty: Literal["warn", "skip", "fail"] = "warn") -> pa.Table:
        """Creates a pyarrow table from source.

        Args:
            if_empty (Literal["warn", "skip", "fail"], optional): : What to do if data
                source contains no data. Defaults to "warn".
        """
        try:
            df = self.to_df(if_empty=if_empty)
        except SKIP:
            return False

        return pa.Table.from_pandas(df)

    def to_csv(
        self,
        path: str,
        if_exists: Literal["append", "replace"] = "replace",
        if_empty: Literal["warn", "skip", "fail"] = "warn",
        sep: str = "\t",
        **kwargs,
    ) -> bool:
        r"""Write from source to a CSV file.

        Note that the source can be a particular file or table,
        but also a database in general. Therefore, some sources may require
        additional parameters to pull the right resource. Hence this method
        passes kwargs to the `to_df()` method implemented by the concrete source.

        Args:
            path (str): The destination path.
            if_exists (Literal[, optional): What to do if the file exists. Defaults to
                "replace".
            if_empty (Literal["warn", "skip", "fail"], optional): What to do if the
                source contains no data. Defaults to "warn".
            sep (str, optional): The separator to use in the CSV. Defaults to "\t".

        Raises:
            ValueError: If the `if_exists` argument is incorrect.

        Returns:
            bool: Whether the operation was successful.
        """
        try:
            df = self.to_df(if_empty=if_empty, **kwargs)
        except SKIP:
            return False

        if if_exists == "append":
            mode = "a"
        elif if_exists == "replace":
            mode = "w"
        else:
            msg = "'if_exists' must be one of ['append', 'replace']"
            raise ValueError(msg)

        df.to_csv(path, sep=sep, mode=mode, index=False, header=not Path(path).exists())

        return True

    def to_excel(
        self,
        path: str,
        if_exists: str = "replace",
        if_empty: Literal["warn", "skip", "fail"] = "warn",
    ) -> bool:
        """Write from source to a excel file.

        Args:
            path (str): The destination path.
            if_exists (str, optional): What to do if the file exists. Defaults to
                "replace".
            if_empty (Literal["warn", "skip", "fail"], optional): What to do if the
                source contains no data.

        """
        try:
            df = self.to_df(if_empty=if_empty)
        except SKIP:
            return False

        if if_exists == "append":
            if Path(path).is_file():
                excel_df = pd.read_excel(path)
                out_df = pd.concat([excel_df, df])
            else:
                out_df = df
        elif if_exists == "replace":
            out_df = df
        out_df.to_excel(path, index=False, encoding="utf8")
        return True

    def _handle_if_empty(
        self,
        if_empty: Literal["warn", "skip", "fail"] = "warn",
        message: str = "The query produced no data.",
    ) -> None:
        """What to do if a fetch (database query, API request) produced no data."""
        if if_empty == "warn":
            self.logger.warning(message)
        elif if_empty == "skip":
            raise SKIP(message)
        elif if_empty == "fail":
            raise ValueError(message)


class SQL(Source):
    def __init__(
        self,
        driver: str | None = None,
        config_key: str | None = None,
        credentials: str | None = None,
        query_timeout: int = 60 * 60,
        *args,
        **kwargs,
    ):
        """A base SQL source class.

        Args:
            driver (str, optional): The SQL driver to use. Defaults to None.
            config_key (str, optional): The key inside local config containing the
                config. User can choose to use this or pass credentials directly to the
                `credentials` parameter. Defaults to None.
            credentials (str, optional): Credentials for the connection. Defaults to
                None.
            query_timeout (int, optional): The timeout for executed queries. Defaults to
                1 hour.
        """
        self.query_timeout = query_timeout

        config_credentials = get_source_credentials(config_key) if config_key else None

        credentials = credentials or config_credentials or {}

        if driver:
            credentials["driver"] = driver

        super().__init__(*args, credentials=credentials, **kwargs)

        self._con = None

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
        uid = self.credentials.get("user") or ""
        pwd = self.credentials.get("password") or ""

        conn_str = f"DRIVER={{{driver}}};SERVER={server};DATABASE={db_name};UID={uid};PWD={pwd};"

        if "authentication" in self.credentials:
            conn_str += "Authentication=" + self.credentials["authentication"] + ";"

        if "trust_server_certificate" in self.credentials:
            conn_str += (
                "TrustServerCertificate="
                + self.credentials["trust_server_certificate"]
                + ";"
            )
        if "encrypt" in self.credentials:
            conn_str += "Encrypt=" + self.credentials["encrypt"] + ";"
        return conn_str

    @property
    def con(self) -> pyodbc.Connection:
        """A singleton-like property for initiating a connection to the database.

        Returns:
            pyodbc.Connection: database connection.
        """
        if not self._con:
            self._con = pyodbc.connect(self.conn_str, timeout=5)
            self._con.timeout = self.query_timeout
        return self._con

    def run(self, query: str) -> list[Record] | bool:
        """Execute a query and return the result.

        Args:
            query (str): The query to execute.

        Returns:
            list[Record] | bool: If the query is a SELECT, return the result as a list
                of records.
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

    def to_df(
        self,
        query: str,
        con: pyodbc.Connection | None = None,
        if_empty: Literal["warn", "skip", "fail"] = "warn",
    ) -> pd.DataFrame:
        """Execute a query and return the result as a pandas DataFrame.

        Args:
            query (str): The query to execute.
            con (pyodbc.Connection, optional): The connection to use to pull the data.
            if_empty (Literal["warn", "skip", "fail"], optional): What to do if the
                query returns no data. Defaults to None.
        """
        conn = con or self.con

        query_sanitized = query.strip().upper()
        if query_sanitized.startswith("SELECT") or query_sanitized.startswith("WITH"):
            df = pd.read_sql_query(query, conn)
            if df.empty:
                self._handle_if_empty(if_empty=if_empty)
        else:
            df = pd.DataFrame()
        return df

    def _check_if_table_exists(self, table: str, schema: str | None = None) -> bool:
        """Check if table exists in a specified schema.

        Args:
            table (str): Table name.
            schema (str, optional): Schema name. Defaults to None.
        """
        exists_query = f"SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = '{schema}' AND TABLE_NAME='{table}'"  # noqa: S608
        return bool(self.run(exists_query))

    def create_table(
        self,
        table: str,
        schema: str | None = None,
        dtypes: dict[str, Any] | None = None,
        if_exists: Literal["fail", "replace", "skip", "delete"] = "fail",
    ) -> bool:
        """Create a table.

        Args:
            table (str): The destination table. Defaults to None.
            schema (str, optional): The destination schema. Defaults to None.
            dtypes (Dict[str, Any], optional): The data types to use for the table.
                Defaults to None.
            if_exists (Literal, optional): What to do if the table already exists.
                Defaults to "fail".

        Returns:
            bool: Whether the operation was successful.
        """
        fqn = f"{schema}.{table}" if schema is not None else table
        exists = self._check_if_table_exists(schema=schema, table=table)

        if exists:
            if if_exists == "replace":
                self.run(f"DROP TABLE {fqn}")
            elif if_exists == "delete":
                self.run(f"DELETE FROM {fqn}")  # noqa: S608
                return True
            elif if_exists == "fail":
                msg = (
                    f"The table {fqn} already exists and 'if_exists' is set to 'fail'."
                )
                raise ValueError(msg)
            elif if_exists == "skip":
                return False

        indent = "  "
        dtypes_rows = [
            indent + f'"{col}"' + " " + dtype for col, dtype in dtypes.items()
        ]
        dtypes_formatted = ",\n".join(dtypes_rows)
        create_table_sql = f"CREATE TABLE {fqn}(\n{dtypes_formatted}\n)"
        self.run(create_table_sql)
        return True

    def insert_into(self, table: str, df: pd.DataFrame) -> str:
        """Insert values from a pandas DataFrame into an existing database table.

        Args:
            table (str): table name
            df (pd.DataFrame): pandas dataframe

        Returns:
            str: The executed SQL insert query.
        """
        values = ""
        rows_count = df.shape[0]
        counter = 0
        for row in df.values:
            counter += 1  # noqa: SIM113
            out_row = ", ".join(map(self._escape_column_name, row))
            comma = ",\n"
            if counter == rows_count:
                comma = ";"
            out_row = f"({out_row}){comma}"
            values += out_row

        columns = ", ".join(df.columns)

        sql = f"INSERT INTO {table} ({columns})\n VALUES {values}"  # noqa: S608
        self.run(sql)

        return sql

    def _escape_column_name(self, column_name: str) -> str:
        """Return an escaped column name."""
        return f"'{column_name}'"
