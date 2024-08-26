"""A module for interacting with DuckDB."""

import re
from typing import Literal

import duckdb
import pandas as pd
from pydantic import BaseModel

from viadot.config import get_source_credentials
from viadot.exceptions import CredentialError
from viadot.signals import SKIP
from viadot.sources.base import Record, Source


class DuckDBCredentials(BaseModel):
    database: str
    read_only: bool = True


class DuckDB(Source):
    DEFAULT_SCHEMA = "main"

    def __init__(
        self,
        config_key: str | None = None,
        credentials: DuckDBCredentials | None = None,
        *args,
        **kwargs,
    ):
        """A class for interacting with DuckDB.

        Args:
            config_key (str, optional): The key inside local config containing the
                credentials.
            credentials (DuckDBCredentials, optional): Credentials for the connection.
                Defaults to None.
            config_key (str, optional): The key in the viadot config holding relevant
                credentials. Defaults to None.
        """
        raw_creds = credentials or get_source_credentials(config_key) or {}
        if credentials is None:
            msg = "Please specify the credentials."
            raise CredentialError(msg)
        validated_creds = dict(
            DuckDBCredentials(**raw_creds)
        )  # validate the credentials
        super().__init__(*args, credentials=validated_creds, **kwargs)

    @property
    def con(self) -> duckdb.DuckDBPyConnection:
        """Return a new connection to the database.

        As the views are highly isolated, we need a new connection for each query in
        order to see the changes from previous queries (eg. if we create a new table and
        then we want to list tables from INFORMATION_SCHEMA, we need to create a new
        DuckDB connection).

        Returns:
            duckdb.DuckDBPyConnection: database connection.
        """
        return duckdb.connect(
            database=self.credentials.get("database"),
            read_only=self.credentials.get("read_only", False),
        )

    @property
    def tables(self) -> list[str]:
        """Show the list of fully qualified table names.

        Returns:
            list[str]: The list of tables in the format '{SCHEMA}.{TABLE}'.
        """
        tables_meta: list[tuple] = self.run_query(
            "SELECT * FROM information_schema.tables"
        )
        return [table_meta[1] + "." + table_meta[2] for table_meta in tables_meta]

    @property
    def schemas(self) -> list[str]:
        """Show the list of schemas.

        Returns:
            list[str]: The list of schemas.
        """
        self.logger.warning(
            "DuckDB does not expose a way to list schemas. `DuckDB.schemas` only contains schemas with tables."
        )
        tables_meta: list[tuple] = self.run_query(
            "SELECT * FROM information_schema.tables"
        )
        return [table_meta[1] for table_meta in tables_meta]

    def to_df(self, query: str, if_empty: str | None = None) -> pd.DataFrame:
        """Run DuckDB query and save output to a pandas DataFrame.

        Args:
            query (str): The query to execute. If query doesn't start with SELECT or
                WITH, empty DataFrame will be returned.
            if_empty (str, optional): What to do if output DataFrame is empty. Defaults
                to None.

        Returns:
            pd.DataFrame: DataFrame with query output
        """
        if query.upper().startswith("SELECT") or query.upper().startswith("WITH"):
            df = self.run_query(query, fetch_type="dataframe")
            if df.empty:
                self._handle_if_empty(if_empty=if_empty)
        else:
            df = pd.DataFrame()
        return df

    def run_query(
        self, query: str, fetch_type: Literal["record", "dataframe"] = "record"
    ) -> list[Record] | bool:
        """Run a query on DuckDB.

        Args:
            query (str): The query to execute.
            fetch_type (Literal[, optional): How to return the data: either in the
                default record format or as a pandas DataFrame. Defaults to "record".

        Returns:
            Union[list[Record], bool]: Either the result set of a query or,
                in case of DDL/DML queries, a boolean describing whether
                the query was executed successfully.
        """
        allowed_fetch_type_values = ["record", "dataframe"]
        if fetch_type not in allowed_fetch_type_values:
            msg = f"Only the values {allowed_fetch_type_values} are allowed for 'fetch_type'"
            raise ValueError(msg)
        cursor = self.con.cursor()
        cursor.execute(query)

        # Cleanup the query.
        query_clean = query.upper().strip()
        # Find comments.
        regex = r"^\s*[--;].*"
        lines = query_clean.splitlines()
        final_query = ""

        for line_raw in lines:
            line = line_raw.strip()
            match_object = re.match(regex, line)
            if not match_object:
                final_query += " " + line
        final_query = final_query.strip()
        query_keywords = ["SELECT", "SHOW", "PRAGMA", "WITH"]
        if any(final_query.startswith(word) for word in query_keywords):
            result = cursor.fetchall() if fetch_type == "record" else cursor.fetchdf()
        else:
            result = True

        cursor.close()
        return result

    def _handle_if_empty(self, if_empty: str = "warn") -> None:
        if if_empty == "warn":
            self.logger.warning("The query produced no data.")
        elif if_empty == "skip":
            msg = "The query produced no data. Skipping..."
            raise SKIP(msg)
        elif if_empty == "fail":
            msg = "The query produced no data."
            raise ValueError(msg)

    def create_table_from_parquet(
        self,
        table: str,
        path: str,
        schema: str | None = None,
        if_exists: Literal["fail", "replace", "append", "skip", "delete"] = "fail",
    ) -> bool:
        """Create a DuckDB table with a CTAS from Parquet file(s).

        Args:
            table (str): Destination table.
            path (str): The path to the source Parquet file(s). Glob expressions are
                also allowed here (eg. `my_folder/*.parquet`).
            schema (str, optional): Destination schema. Defaults to None.
            if_exists (Literal[, optional): What to do if the table already exists.
            The 'delete' option deletes data and then inserts new one. Defaults to
                "fail".

        Raises:
            ValueError: If the table exists and `if_exists` is set to `fail`.

        Returns:
            None: Does not return anything.
        """
        schema = schema or DuckDB.DEFAULT_SCHEMA
        fqn = schema + "." + table
        exists = self._check_if_table_exists(schema=schema, table=table)

        if exists:
            if if_exists == "replace":
                self.run_query(f"DROP TABLE {fqn}")
            elif if_exists == "append":
                self.logger.info(f"Appending to table {fqn}...")
                create_table_query = f"COPY {fqn} FROM '{path}' (FORMAT 'parquet')"
                self.run_query(create_table_query)
                self.logger.info(f"Successfully appended data to table '{fqn}'.")
                return True
            elif if_exists == "delete":
                self.run_query(f"DELETE FROM {fqn}")  # noqa: S608
                self.logger.info(f"Successfully deleted data from table '{fqn}'.")
                self.run_query(
                    f"INSERT INTO {fqn} SELECT * FROM read_parquet('{path}')"  # noqa: S608
                )
                self.logger.info(f"Successfully inserted data into table '{fqn}'.")
                return True
            elif if_exists == "fail":
                msg = "The table already exists and 'if_exists' is set to 'fail'."
                raise ValueError(msg)
            elif if_exists == "skip":
                return False
        self.run_query(f"CREATE SCHEMA IF NOT EXISTS {schema}")
        self.logger.info(f"Creating table {fqn}...")
        create_table_query = f"CREATE TABLE {fqn} AS SELECT * FROM '{path}';"  # noqa: S608
        self.run_query(create_table_query)
        self.logger.info(f"Table {fqn} has been created successfully.")
        return True

    def drop_table(self, table: str, schema: str | None = None) -> bool:
        """Drop a table.

        A thin wrapper around DuckDB.run_query(), with additional logs.

        Args:
            table (str): The table to be dropped.
            schema (str, optional): The schema where the table is located. Defaults to
                None.

        Returns:
            bool: Whether the table was dropped.
        """
        schema = schema or DuckDB.DEFAULT_SCHEMA
        fqn = schema + "." + table

        self.logger.info(f"Dropping table {fqn}...")
        dropped = self.run_query(f"DROP TABLE IF EXISTS {fqn}")
        if dropped:
            self.logger.info(f"Table {fqn} has been dropped successfully.")
        else:
            self.logger.info(f"Table {fqn} could not be dropped.")
        return dropped

    def _check_if_table_exists(self, table: str, schema: str | None = None) -> bool:
        schema = schema or DuckDB.DEFAULT_SCHEMA
        fqn = schema + "." + table
        return fqn in self.tables

    def _check_if_schema_exists(self, schema: str) -> bool:
        if schema == DuckDB.DEFAULT_SCHEMA:
            return True
        fqns = self.tables
        return any(fqn.split(".")[0] == schema for fqn in fqns)
