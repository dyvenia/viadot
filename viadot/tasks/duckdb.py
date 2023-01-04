from typing import Any, List, Literal, NoReturn, Tuple, Union

import pandas as pd
from prefect import Task
from prefect.utilities.tasks import defaults_from_attrs

from ..signals import SKIP
from ..sources import DuckDB
from ..utils import check_if_empty_file

Record = Tuple[Any]


class DuckDBQuery(Task):
    """
    Task for running a query on DuckDB.

    Args:
        credentials (dict, optional): The config to use for connecting with the db.
        timeout(int, optional): The amount of time (in seconds) to wait while running this task before
            a timeout occurs. Defaults to 3600.
    """

    def __init__(
        self,
        credentials: dict = None,
        timeout: int = 3600,
        *args,
        **kwargs,
    ):
        self.credentials = credentials
        super().__init__(name="run_duckdb_query", timeout=timeout, *args, **kwargs)

    @defaults_from_attrs("credentials")
    def run(
        self,
        query: str,
        fetch_type: Literal["record", "dataframe"] = "record",
        credentials: dict = None,
    ) -> Union[List[Record], bool]:
        """Run a query on DuckDB.

        Args:
            query (str, required):  The query to execute.
            fetch_type (Literal[, optional): How to return the data: either
            in the default record format or as a pandas DataFrame. Defaults to "record".
            credentials (dict, optional): The config to use for connecting with the db.

        Returns:
            Union[List[Record], bool]: Either the result set of a query or,
            in case of DDL/DML queries, a boolean describing whether
            the query was excuted successfuly.
        """

        duckdb = DuckDB(credentials=credentials)

        # run the query and fetch the results if it's a select
        result = duckdb.run(query, fetch_type=fetch_type)

        self.logger.info(f"Successfully ran the query.")
        return result


class DuckDBCreateTableFromParquet(Task):
    """
    Task for creating a DuckDB table with a CTAS from Parquet file(s).

    Args:
        table (str, optional): Destination table.
        also allowed here (eg. `my_folder/*.parquet`).
        schema (str, optional): Destination schema.
        if_exists (Literal, optional): What to do if the table already exists.
        if_empty (Literal, optional): What to do if ".parquet" file is emty. Defaults to "skip".
        credentials(dict, optional): The config to use for connecting with the db.
        timeout(int, optional): The amount of time (in seconds) to wait while running this task before
            a timeout occurs. Defaults to 3600.

    Raises:
        ValueError: If the table exists and `if_exists`is set to `fail` or when parquet file
        is empty and `if_empty` is set to `fail`.

    Returns:
        NoReturn: Does not return anything.
    """

    def __init__(
        self,
        schema: str = None,
        if_exists: Literal["fail", "replace", "append", "skip", "delete"] = "fail",
        if_empty: Literal["skip", "fail"] = "skip",
        credentials: dict = None,
        timeout: int = 3600,
        *args,
        **kwargs,
    ):
        self.schema = schema
        self.if_exists = if_exists
        self.if_empty = if_empty
        self.credentials = credentials

        super().__init__(
            name="duckdb_create_table",
            timeout=timeout,
            *args,
            **kwargs,
        )

    @defaults_from_attrs("schema", "if_exists", "if_empty")
    def run(
        self,
        table: str,
        path: str,
        schema: str = None,
        if_exists: Literal["fail", "replace", "append", "skip", "delete"] = None,
        if_empty: Literal["skip", "fail"] = None,
    ) -> NoReturn:
        """
        Create a DuckDB table with a CTAS from Parquet file(s).

        Args:
            table (str, optional): Destination table.
            path (str): The path to the source Parquet file(s). Glob expressions are
            also allowed here (eg. `my_folder/*.parquet`).
            schema (str, optional): Destination schema.
            if_exists (Literal, optional): What to do if the table already exists.
            if_empty (Literal, optional): What to do if Parquet file is empty. Defaults to None.

        Raises:
            ValueError: If the table exists and `if_exists`is set to `fail` or when parquet file
            is empty and `if_empty` is set to `fail`.

        Returns:
            NoReturn: Does not return anything.
        """
        try:
            check_if_empty_file(path=path, if_empty=if_empty)
        except SKIP:
            self.logger.info("The input file is empty. Skipping.")
            return

        duckdb = DuckDB(credentials=self.credentials)

        fqn = f"{schema}.{table}" if schema is not None else table
        created = duckdb.create_table_from_parquet(
            path=path, schema=schema, table=table, if_exists=if_exists
        )
        if created:
            self.logger.info(f"Successfully created table {fqn}.")
        else:
            self.logger.info(
                f"Table {fqn} has not been created as if_exists is set to {if_exists}."
            )


class DuckDBToDF(Task):
    """
    Load a table from DuckDB to a pandas DataFrame.

    Args:
        schema (str, optional): Source schema.
        table (str, optional): Source table.
        if_empty (Literal[, optional): What to do if the query returns no data.
        Defaults to "warn".
        credentials (dict, optional): The config to use for connecting with the db.
        timeout(int, optional): The amount of time (in seconds) to wait while running this task before
            a timeout occurs. Defaults to 3600.

    Returns:
        pd.DataFrame: a pandas DataFrame containing the table data.
    """

    def __init__(
        self,
        schema: str = None,
        table: str = None,
        if_empty: Literal["warn", "skip", "fail"] = "warn",
        credentials: dict = None,
        timeout: int = 3600,
        *args,
        **kwargs,
    ):

        self.schema = schema
        self.table = table
        self.if_empty = if_empty
        self.credentials = credentials

        super().__init__(name="duckdb_to_df", timeout=timeout, *args, **kwargs)

    @defaults_from_attrs("schema", "table", "if_empty", "credentials")
    def run(
        self,
        schema: str = None,
        table: str = None,
        if_empty: Literal["warn", "skip", "fail"] = None,
        credentials: dict = None,
    ) -> pd.DataFrame:
        """Load a DuckDB table into a pandas DataFrame.

        Args:
            schema (str, optional): Source schema.
            table (str, optional): Source table.
            if_empty (Literal[, optional): What to do if the query returns no data.
            Defaults to "warn".
            credentials (dict, optional): The config to use for connecting with the db.

        Returns:
            pd.DataFrame: a pandas DataFrame containing the table data.
        """

        if table is None:
            raise ValueError("Table is required.")

        duckdb = DuckDB(credentials=credentials)

        # run the query and fetch the results if it's a select
        fqn = f"{schema}.{table}" if schema is not None else table
        query = f"SELECT * FROM {fqn}"
        df = duckdb.to_df(query, if_empty=if_empty)

        self.logger.info(f"Data has been loaded sucessfully.")
        return df
