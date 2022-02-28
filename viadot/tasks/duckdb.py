from typing import Any, List, Literal, Tuple, Union, NoReturn

from prefect import Task
from prefect.utilities.tasks import defaults_from_attrs
import pandas as pd

from ..sources import DuckDB

Record = Tuple[Any]


class DuckDBQuery(Task):
    """
    Task for running a query on DuckDB.

    Args:
        credentials (dict, optional): The config to use for connecting with the db.
    """

    def __init__(
        self,
        credentials: dict = None,
        *args,
        **kwargs,
    ):
        self.credentials = credentials
        super().__init__(name="run_duckdb_query", *args, **kwargs)

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
        credentials(dict, optional): The config to use for connecting with the db.

    Raises:
        ValueError: If the table exists and `if_exists` is set to `fail`.

    Returns:
        NoReturn: Does not return anything.
    """

    def __init__(
        self,
        schema: str = None,
        if_exists: Literal["fail", "replace", "append", "skip", "delete"] = "fail",
        credentials: dict = None,
        *args,
        **kwargs,
    ):
        self.schema = schema
        self.if_exists = if_exists
        self.credentials = credentials

        super().__init__(
            name="duckdb_create_table",
            *args,
            **kwargs,
        )

    @defaults_from_attrs("schema", "if_exists")
    def run(
        self,
        table: str,
        path: str,
        schema: str = None,
        if_exists: Literal["fail", "replace", "append", "skip", "delete"] = None,
    ) -> NoReturn:
        """
        Create a DuckDB table with a CTAS from Parquet file(s).

        Args:
            table (str, optional): Destination table.
            path (str): The path to the source Parquet file(s). Glob expressions are
            also allowed here (eg. `my_folder/*.parquet`).
            schema (str, optional): Destination schema.
            if_exists (Literal, optional): What to do if the table already exists.

        Raises:
            ValueError: If the table exists and `if_exists` is set to `fail`.

        Returns:
            NoReturn: Does not return anything.
        """

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

    Returns:
        pd.DataFrame: a pandas DataFrame containing the table data.
    """

    def __init__(
        self,
        schema: str = None,
        table: str = None,
        if_empty: Literal["warn", "skip", "fail"] = "warn",
        credentials: dict = None,
        *args,
        **kwargs,
    ):

        self.schema = schema
        self.table = table
        self.if_empty = if_empty
        self.credentials = credentials

        super().__init__(name="duckdb_to_df", *args, **kwargs)

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
