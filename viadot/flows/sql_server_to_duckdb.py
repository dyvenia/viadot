from typing import Any, Dict, List, Literal

from prefect import Flow

from viadot.task_utils import add_ingestion_metadata_task, cast_df_to_str, df_to_parquet
from viadot.tasks import DuckDBCreateTableFromParquet, SQLServerToDF


class SQLServerToDuckDB(Flow):
    def __init__(
        self,
        name,
        sql_query: str,
        local_file_path: str,
        sqlserver_config_key: str = None,
        duckdb_table: str = None,
        duckdb_schema: str = None,
        if_exists: Literal["fail", "replace", "append", "skip", "delete"] = "fail",
        if_empty: Literal["warn", "skip", "fail"] = "skip",
        duckdb_credentials: dict = None,
        timeout: int = 3600,
        *args: List[any],
        **kwargs: Dict[str, Any],
    ):
        """
        Flow for upolading data from SQL Server to DuckDB.

        Args:
            name (str): The name of the flow.
            sql_query (str, required): The query to execute on the SQL Server database. If don't start with "SELECT"
                returns empty DataFrame.
            local_file_path (str): Path to output parquet file.
            sqlserver_config_key (str, optional): The key inside local config containing the credentials. Defaults to None.
            duckdb_table (str, optional): Destination table in DuckDB. Defaults to None.
            duckdb_schema (str, optional): Destination schema in DuckDB. Defaults to None.
            if_exists (Literal, optional):  What to do if the table already exists. Defaults to "fail".
            if_empty (Literal, optional): What to do if Parquet file is empty. Defaults to "skip".
            duckdb_credentials (dict, optional): Credentials for the DuckDB connection. Defaults to None.
            timeout(int, optional): The amount of time (in seconds) to wait while running this task before
                a timeout occurs. Defaults to 3600.
        """
        # SQLServerToDF
        self.sql_query = sql_query
        self.sqlserver_config_key = sqlserver_config_key
        self.timeout = timeout

        # DuckDBCreateTableFromParquet
        self.local_file_path = local_file_path
        self.duckdb_table = duckdb_table
        self.duckdb_schema = duckdb_schema
        self.if_exists = if_exists
        self.if_empty = if_empty
        self.duckdb_credentials = duckdb_credentials

        super().__init__(*args, name=name, **kwargs)

        self.create_duckdb_table_task = DuckDBCreateTableFromParquet(
            credentials=duckdb_credentials, timeout=timeout
        )

        self.gen_flow()

    def gen_flow(self) -> Flow:
        df_task = SQLServerToDF(timeout=self.timeout)
        df = df_task.bind(
            config_key=self.sqlserver_config_key, query=self.sql_query, flow=self
        )
        df_mapped = cast_df_to_str.bind(df, flow=self)
        df_with_metadata = add_ingestion_metadata_task.bind(df_mapped, flow=self)
        parquet = df_to_parquet.bind(
            df=df_with_metadata,
            path=self.local_file_path,
            if_exists=self.if_exists,
            flow=self,
        )
        create_duckdb_table = self.create_duckdb_table_task.bind(
            path=self.local_file_path,
            schema=self.duckdb_schema,
            table=self.duckdb_table,
            if_exists=self.if_exists,
            if_empty=self.if_empty,
            flow=self,
        )
        create_duckdb_table.set_upstream(parquet, flow=self)
