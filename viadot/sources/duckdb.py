from typing import Any, List, Literal, NoReturn, Tuple, Union
import duckdb
import pandas as pd
import re
from prefect.utilities import logging

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
        credentials: dict = None,
        *args,
        **kwargs,
    ):
        """A class for interacting with DuckDB.

        Args:
            config_key (str, optional): The key inside local config containing the config.
            User can choose to use this or pass credentials directly to the `credentials`
            parameter. Defaults to None.
            credentials (dict, optional): Credentials for the connection. Defaults to None.
        """

        if config_key:
            config_credentials = local_config.get(config_key)

        credentials = credentials if credentials else config_credentials
        if credentials is None:
            raise CredentialError("Credentials not found.")

        super().__init__(*args, credentials=credentials, **kwargs)

    @property
    def con(self) -> duckdb.DuckDBPyConnection:
        """Return a new connection to the database. As the views are highly isolated,
        we need a new connection for each query in order to see the changes from
        previous queries (eg. if we create a new table and then we want to list
        tables from INFORMATION_SCHEMA, we need to create a new DuckDB connection).

        Returns:
            duckdb.DuckDBPyConnection: database connection.
        """
        return duckdb.connect(
            database=self.credentials.get("database"),
            read_only=self.credentials.get("read_only", False),
        )

    @property
    def tables(self) -> List[str]:
        """Show the list of fully qualified table names.

        Returns:
            List[str]: The list of tables in the format '{SCHEMA}.{TABLE}'.
        """
        tables_meta: List[Tuple] = self.run("SELECT * FROM information_schema.tables")
        tables = [table_meta[1] + "." + table_meta[2] for table_meta in tables_meta]
        return tables

    @property
    def schemas(self) -> List[str]:
        """Show the list of schemas.

        Returns:
            List[str]: The list ofschemas.
        """
        self.logger.warning(
            "DuckDB does not expose a way to list schemas. `DuckDB.schemas` only contains schemas with tables."
        )
        tables_meta: List[Tuple] = self.run("SELECT * FROM information_schema.tables")
        schemas = [table_meta[1] for table_meta in tables_meta]
        return schemas

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
        """Run a query on DuckDB.

        Args:
            query (str): The query to execute.
            fetch_type (Literal[, optional): How to return the data: either
            in the default record format or as a pandas DataFrame. Defaults to "record".

        Returns:
            Union[List[Record], bool]: Either the result set of a query or,
            in case of DDL/DML queries, a boolean describing whether
            the query was excuted successfuly.
        """
        allowed_fetch_type_values = ["record", "dataframe"]
        if fetch_type not in allowed_fetch_type_values:
            raise ValueError(
                f"Only the values {allowed_fetch_type_values} are allowed for 'fetch_type'"
            )
        cursor = self.con.cursor()
        cursor.execute(query)

        query_clean = query.upper().strip()
        regex = r"^\s*[--;].*"
        lines = query_clean.splitlines()
        final_query = ""

        for line in lines:
            line = line.strip()
            match_object = re.match(regex, line)
            if not match_object:
                final_query += " " + line
        final_query = final_query.strip()
        query_keywords = ["SELECT", "SHOW", "PRAGMA", "WITH"]
        if any(final_query.startswith(word) for word in query_keywords):
            if fetch_type == "record":
                result = cursor.fetchall()
            else:
                result = cursor.fetchdf()
        else:
            result = True

        cursor.close()
        return result

    def _handle_if_empty(self, if_empty: str = "warn") -> NoReturn:
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
        if_exists: Literal["fail", "replace", "append", "skip", "delete"] = "fail",
    ) -> NoReturn:
        """Create a DuckDB table with a CTAS from Parquet file(s).

        Args:
            table (str): Destination table.
            path (str): The path to the source Parquet file(s). Glob expressions are
            also allowed here (eg. `my_folder/*.parquet`).
            schema (str, optional): Destination schema. Defaults to None.
            if_exists (Literal[, optional): What to do if the table already exists. Defaults to "fail".

        Raises:
            ValueError: If the table exists and `if_exists` is set to `fail`.

        Returns:
            NoReturn: Does not return anything.
        """
        schema = schema or DuckDB.DEFAULT_SCHEMA
        fqn = schema + "." + table
        exists = self._check_if_table_exists(schema=schema, table=table)

        if exists:
            if if_exists == "replace":
                self.run(f"DROP TABLE {fqn}")
            elif if_exists == "append":
                self.logger.info(f"Appending to table {fqn}...")
                ingest_query = f"COPY {fqn} FROM '{path}' (FORMAT 'parquet')"
                self.run(ingest_query)
                self.logger.info(f"Successfully appended data to table '{fqn}'.")
                return True
            elif if_exists == "delete":
                self.run(f"DELETE FROM {fqn}")
                self.logger.info(f"Successfully deleted data from table '{fqn}'.")
                self.run(f"INSERT INTO {fqn} SELECT * FROM read_parquet('{path}')")
                self.logger.info(f"Successfully inserted data into table '{fqn}'.")
                return True
            elif if_exists == "fail":
                raise ValueError(
                    "The table already exists and 'if_exists' is set to 'fail'."
                )
            elif if_exists == "skip":
                return False

        self.run(f"CREATE SCHEMA IF NOT EXISTS {schema}")

        self.logger.info(f"Creating table {fqn}...")
        ingest_query = f"CREATE TABLE {fqn} AS SELECT * FROM '{path}';"
        self.run(ingest_query)
        self.logger.info(f"Table {fqn} has been created successfully.")

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

        schema = schema or DuckDB.DEFAULT_SCHEMA
        fqn = schema + "." + table

        self.logger.info(f"Dropping table {fqn}...")
        dropped = self.run(f"DROP TABLE IF EXISTS {fqn}")
        if dropped:
            self.logger.info(f"Table {fqn} has been dropped successfully.")
        else:
            self.logger.info(f"Table {fqn} could not be dropped.")
        return dropped

    def _check_if_table_exists(self, table: str, schema: str = None) -> bool:
        schema = schema or DuckDB.DEFAULT_SCHEMA
        fqn = schema + "." + table
        return fqn in self.tables

    def _check_if_schema_exists(self, schema: str) -> bool:
        if schema == self.DEFAULT_SCHEMA:
            return True
        fqns = self.tables
        return any((fqn.split(".")[0] == schema for fqn in fqns))
