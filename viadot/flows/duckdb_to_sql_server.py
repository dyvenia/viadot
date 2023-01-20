import os
from typing import Any, Dict, List, Literal

import prefect
from prefect import Flow, task
from prefect.utilities import logging

from viadot.task_utils import df_to_csv as df_to_csv_task
from viadot.task_utils import get_sql_dtypes_from_df as get_sql_dtypes_from_df_task
from viadot.tasks import BCPTask, DuckDBToDF, SQLServerCreateTable, DuckDBQuery

logger = logging.get_logger(__name__)


@task(timeout=3600)
def cleanup_csv_task(path: str):

    logger = prefect.context.get("logger")

    logger.info(f"Removing file {path}...")
    try:
        os.remove(path)
        logger.info(f"File {path} has been successfully removed.")
        return True
    except Exception as e:
        logger.exception(f"File {path} could not be removed.")
        return False


class DuckDBToSQLServer(Flow):
    def __init__(
        self,
        name: str,
        duckdb_schema: str = None,
        duckdb_table: str = None,
        if_empty: str = "warn",
        duckdb_query: str = None,
        duckdb_credentials: dict = None,
        local_file_path: str = None,
        write_sep: str = "\t",
        sql_server_schema: str = None,
        sql_server_table: str = None,
        dtypes: Dict[str, Any] = None,
        if_exists: Literal["fail", "replace", "append", "delete"] = "replace",
        sql_server_credentials: dict = None,
        on_bcp_error: Literal["skip", "fail"] = "skip",
        bcp_error_log_path="./log_file.log",
        tags: List[str] = ["load"],
        timeout: int = 3600,
        *args: List[any],
        **kwargs: Dict[str, Any],
    ):
        """
        Flow for moving a table from DuckDB to SQL Server. User can specify schema and table to transfer
        data between databases or use a SELECT query instead.

        Args:
            name (str): The name of the flow.
            duckdb_schema (str, optional): Destination schema. Defaults to None.
            duckdb_table (str, optional): Destination table. Defaults to None.
            if_empty (str, optional): What to do if the query returns no data. Defaults to "warn".
            duckdb_query (str, optional): SELECT query to be executed on DuckDB, its result will be used to
            create table. Defaults to None.
            duckdb_credentials (dict, optional): The config to use for connecting with DuckDB.
            local_file_path (str, optional): Local destination path. Defaults to None.
            write_sep (str, optional): The delimiter for the output CSV file. Defaults to "\t".
            sql_server_schema (str, optional): Destination schema. Defaults to None.
            sql_server_table (str, optional): Destination table. Defaults to None.
            dtypes (dict, optional): The data types to be enforced for the resulting table. By default,
            we infer them from the DataFrame. Defaults to None.
            if_exists (Literal, optional): What to do if the table exists. Defaults to "replace".
            sql_server_credentials (dict, optional): The credentials to use for connecting with SQL Server.
            on_bcp_error (Literal["skip", "fail"], optional): What to do if error occurs. Defaults to "skip".
            bcp_error_log_path (string, optional): Full path of an error file. Defaults to "./log_file.log".
            tags (List[str], optional): Flow tags to use, eg. to control flow concurrency. Defaults to ["load"].
            timeout(int, optional): The amount of time (in seconds) to wait while running this task before
                a timeout occurs. Defaults to 3600.
        """

        # DuckDBToDF
        self.duckdb_schema = duckdb_schema
        self.duckdb_table = duckdb_table
        self.duckdb_credentials = duckdb_credentials

        # DuckDBQuery
        self.duckdb_query = duckdb_query

        # df_to_csv_task
        self.local_file_path = local_file_path or self.slugify(name) + ".csv"
        self.write_sep = write_sep
        self.if_empty = if_empty

        # SQLServerCreateTable
        self.sql_server_table = sql_server_table
        self.sql_server_schema = sql_server_schema
        self.dtypes = dtypes
        self.if_exists = self._map_if_exists(if_exists)
        self.sql_server_credentials = sql_server_credentials
        self.on_bcp_error = on_bcp_error
        self.bcp_error_log_path = bcp_error_log_path

        # Global
        self.tags = tags
        self.timeout = timeout

        super().__init__(*args, name=name, **kwargs)

        self.duckdb_run_query_task = DuckDBQuery(credentials=duckdb_credentials)

        self.gen_flow()

    @staticmethod
    def _map_if_exists(if_exists: str) -> str:
        mapping = {"append": "skip"}
        return mapping.get(if_exists, if_exists)

    @staticmethod
    def slugify(name):
        return name.replace(" ", "_").lower()

    def gen_flow(self) -> Flow:
        if self.duckdb_query is None:
            duckdb_to_df_task = DuckDBToDF(timeout=self.timeout)
            df = duckdb_to_df_task.bind(
                schema=self.duckdb_schema,
                table=self.duckdb_table,
                if_empty=self.if_empty,
                credentials=self.duckdb_credentials,
                flow=self,
            )
        else:
            df = self.duckdb_run_query_task.bind(
                query=self.duckdb_query,
                credentials=self.duckdb_credentials,
                fetch_type="dataframe",
                flow=self,
            )

        df_to_csv = df_to_csv_task.bind(
            df=df,
            path=self.local_file_path,
            sep=self.write_sep,
            flow=self,
        )
        if self.dtypes:
            # Use user-provided dtypes.
            dtypes = self.dtypes
        else:
            dtypes = get_sql_dtypes_from_df_task.bind(df=df, flow=self)

        create_table_task = SQLServerCreateTable(timeout=self.timeout)
        create_table_task.bind(
            schema=self.sql_server_schema,
            table=self.sql_server_table,
            dtypes=dtypes,
            if_exists=self.if_exists,
            credentials=self.sql_server_credentials,
            flow=self,
        )

        bulk_insert_task = BCPTask(timeout=self.timeout)
        bulk_insert_task.bind(
            path=self.local_file_path,
            schema=self.sql_server_schema,
            table=self.sql_server_table,
            credentials=self.sql_server_credentials,
            on_error=self.on_bcp_error,
            error_log_file_path=self.bcp_error_log_path,
            flow=self,
        )
        cleanup_csv_task.bind(path=self.local_file_path, flow=self)

        create_table_task.set_upstream(df_to_csv, flow=self)
        bulk_insert_task.set_upstream(create_table_task, flow=self)
        cleanup_csv_task.set_upstream(bulk_insert_task, flow=self)
