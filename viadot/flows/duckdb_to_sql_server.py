import os
from typing import Any, Dict, List, Literal

import prefect
from prefect import Flow, task
from prefect.utilities import logging

from ..task_utils import df_to_csv as df_to_csv_task
from ..task_utils import get_sql_dtypes_from_df as get_sql_dtypes_from_df_task
from ..tasks import BCPTask, DuckDBToDF, SQLServerCreateTable

logger = logging.get_logger(__name__)

duckdb_to_df_task = DuckDBToDF()
create_table_task = SQLServerCreateTable()
bulk_insert_task = BCPTask()


@task
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
        duckdb_credentials: dict = None,
        local_file_path: str = None,
        write_sep: str = "\t",
        sql_server_schema: str = None,
        sql_server_table: str = None,
        dtypes: Dict[str, Any] = None,
        if_exists: Literal["fail", "replace", "append", "delete"] = "replace",
        sql_server_credentials: dict = None,
        tags: List[str] = ["load"],
        *args: List[any],
        **kwargs: Dict[str, Any],
    ):
        """
        Flow for moving a table from DuckDB to SQL Server.

        Args:
            name (str): The name of the flow.
            duckdb_schema (str, optional): Destination schema. Defaults to None.
            duckdb_table (str, optional): Destination table. Defaults to None.
            if_empty (str, optional): What to do if the query returns no data. Defaults to "warn".
            duckdb_credentials (dict, optional): The config to use for connecting with DuckDB.
            local_file_path (str, optional): Local destination path. Defaults to None.
            write_sep (str, optional): The delimiter for the output CSV file. Defaults to "\t".
            sql_server_schema (str, optional): Destination schema. Defaults to None.
            sql_server_table (str, optional): Destination table. Defaults to None.
            dtypes (dict, optional): The data types to be enforced for the resulting table. By default,
            we infer them from the DataFrame. Defaults to None.
            if_exists (Literal, optional): What to do if the table exists. Defaults to "replace".
            sql_server_credentials (dict, optional): The credentials to use for connecting with SQL Server.
            tags (List[str], optional): Flow tags to use, eg. to control flow concurrency. Defaults to ["load"].
        """

        # DuckDBToDF
        self.duckdb_schema = duckdb_schema
        self.duckdb_table = duckdb_table
        self.duckdb_credentials = duckdb_credentials

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

        # Global
        self.tags = tags

        super().__init__(*args, name=name, **kwargs)

        self.gen_flow()

    @staticmethod
    def _map_if_exists(if_exists: str) -> str:
        mapping = {"append": "skip"}
        return mapping.get(if_exists, if_exists)

    @staticmethod
    def slugify(name):
        return name.replace(" ", "_").lower()

    def gen_flow(self) -> Flow:
        df = duckdb_to_df_task.bind(
            schema=self.duckdb_schema,
            table=self.duckdb_table,
            if_empty=self.if_empty,
            credentials=self.duckdb_credentials,
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

        create_table_task.bind(
            schema=self.sql_server_schema,
            table=self.sql_server_table,
            dtypes=dtypes,
            if_exists=self.if_exists,
            credentials=self.sql_server_credentials,
            flow=self,
        )
        bulk_insert_task.bind(
            path=self.local_file_path,
            schema=self.sql_server_schema,
            table=self.sql_server_table,
            credentials=self.sql_server_credentials,
            flow=self,
        )
        cleanup_csv_task.bind(path=self.local_file_path, flow=self)

        create_table_task.set_upstream(df_to_csv, flow=self)
        bulk_insert_task.set_upstream(create_table_task, flow=self)
        cleanup_csv_task.set_upstream(bulk_insert_task, flow=self)
