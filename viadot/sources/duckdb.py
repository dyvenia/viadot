from typing import List, Literal, NoReturn, Tuple, Any, Union

import pandas as pd
from prefect.utilities import logging

import duckdb

from ..config import local_config
from ..exceptions import CredentialError
from ..signals import SKIP
from .base import Source

logger = logging.get_logger(__name__)

Record = Tuple[Any]


class DuckDB(Source):
    DEFAULT_SCHEMA = "main"

    def __init__(
        self,
        config_key: str = "DuckDB",
        credentials: str = None,
        *args,
        **kwargs,
    ):
        """A class for interacting with DuckDB.

        Args:
            config_key (str, optional): The key inside local config containing the config.
            User can choose to use this or pass credentials directly to the `credentials`
            parameter. Defaults to None.
            credentials (str, optional): Credentials for the connection. Defaults to None.
        """

        if config_key:
            config_credentials = local_config.get(config_key)

        credentials = credentials if credentials else config_credentials
        if credentials is None:
            raise CredentialError("Credentials not found.")

        super().__init__(*args, credentials=credentials, **kwargs)

        self._con = None

    @property
    def con(self) -> duckdb.DuckDBPyConnection:
        """A singleton-like property for initiating a connection to the database.

        Returns:
            duckdb.DuckDBPyConnection: database connection.
        """
        if not self._con:
            self._con = duckdb.connect(
                database=self.credentials.get("database"),
                read_only=self.credentials.get("read_only", False),
            )
        return self._con

    @property
    def tables(self) -> List[str]:
        """Show the list of tables (fully qualified).

        Returns:
            List[str]: The list of tables in the format '{SCHEMA}.{TABLE}'.
        """
        tables_meta: List[Tuple] = self.run("SELECT * FROM information_schema.tables")
        tables = [table_meta[1] + "." + table_meta[2] for table_meta in tables_meta]
        return tables

    def to_df(self, query: str, if_empty: str = None) -> pd.DataFrame:
        if query.upper().startswith("SELECT"):
            df = self.run(query, fetch_type="dataframe")
            if df.empty:
                self._handle_if_empty(if_empty=if_empty)
        else:
            df = pd.DataFrame()
        return df

    def run(
        self, query: str, fetch_type: Literal["record", "dataframe"] = "record"
    ) -> Union[List[Record], bool]:
        cursor = self.con.cursor()
        cursor.execute(query)

        query_clean = query.upper().strip()
        query_keywords = ["SELECT", "SHOW", "PRAGMA"]
        if any(query_clean.startswith(word) for word in query_keywords):
            if fetch_type == "record":
                result = cursor.fetchall()
            else:
                result = cursor.fetchdf()
        else:
            result = True

        cursor.close()
        return result

    def _handle_if_empty(self, if_empty: str = None) -> NoReturn:
        if if_empty == "warn":
            logger.warning("The query produced no data.")
        elif if_empty == "skip":
            raise SKIP("The query produced no data. Skipping...")
        elif if_empty == "fail":
            raise ValueError("The query produced no data.")

    def create_table_from_parquet(
        self,
        table: str,
        path: str,
        schema: str = None,
        if_exists: Literal["fail", "replace", "skip", "delete"] = "fail",
    ) -> NoReturn:
        schema = schema or DuckDB.DEAULT_SCHEMA
        fqn = schema + "." + table
        exists = self._check_if_table_exists(schema=schema, table=table)

        if exists:
            if if_exists == "replace":
                self.run(f"DROP TABLE {fqn}")
            elif if_exists == "delete":
                self.run(f"DELETE FROM {fqn}")
                return True
            elif if_exists == "fail":
                raise ValueError(
                    "The table already exists and 'if_exists' is set to 'fail'."
                )
            elif if_exists == "skip":
                return False

        schema_exists = self._check_if_schema_exists(schema)
        if not schema_exists:
            self.run(f"CREATE SCHEMA {schema}")

        self.logger.info(f"Creating table {fqn}...")
        ingest_query = f"CREATE TABLE {fqn} AS SELECT * FROM '{path}';"
        self.run(ingest_query)
        self.logger.info(f"Table {fqn} has been created successfully.")

    def insert_into_from_parquet():
        # check with Marcin if needed
        pass

    def drop_table(self, table: str, schema: str = None) -> bool:
        """
        Drop a table.

        This is a thin wraper around DuckDB.run() which logs to the operation.

        Args:
            table (str): The table to be dropped.
            schema (str, optional): The schema where the table is located.
            Defaults to None.

        Returns:
            bool: Whether the table was dropped.
        """

        schema = schema or DuckDB.DEAULT_SCHEMA
        fqn = schema + "." + table

        self.logger.info(f"Dropping table {fqn}...")
        dropped = self.run(f"DROP TABLE IF EXISTS {fqn}")
        if dropped:
            self.logger.info(f"Table {fqn} has been dropped successfully.")
        else:
            self.logger.info(f"Table {fqn} could not be dropped.")
        return dropped

    def _check_if_table_exists(self, table: str, schema: str = None) -> bool:
        schema = schema or DuckDB.DEAULT_SCHEMA
        fqn = schema + "." + table
        return fqn in self.tables

    def _check_if_schema_exists(self, schema: str) -> bool:
        if schema == self.DEFAULT_SCHEMA:
            return True
        fqns = self.tables
        return any((fqn.split(".")[0] == schema for fqn in fqns))
