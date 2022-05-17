from prefect import Flow
from typing import Any, Dict, List, Literal


from ..task_utils import df_to_parquet, add_ingestion_metadata_task
from ..tasks import SQLServerToDF, DuckDBCreateTableFromParquet

df_task = SQLServerToDF()


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
        duckdb_credentials: dict = None,
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
            duckdb_credentials (dict, optional): Credentials for the DuckDB connection. Defaults to None.

        """
        # SQLServerToDF
        self.sql_query = sql_query
        self.sqlserver_config_key = sqlserver_config_key

        # DuckDBCreateTableFromParquet
        self.local_file_path = local_file_path
        self.duckdb_table = duckdb_table
        self.duckdb_schema = duckdb_schema
        self.if_exists = if_exists
        self.duckdb_credentials = duckdb_credentials

        super().__init__(*args, name=name, **kwargs)

        self.create_duckdb_table_task = DuckDBCreateTableFromParquet(
            credentials=duckdb_credentials
        )

        self.gen_flow()

    def gen_flow(self) -> Flow:
        df = df_task.bind(
            config_key=self.sqlserver_config_key, query=self.sql_query, flow=self
        )
        df_with_metadata = add_ingestion_metadata_task.bind(df, flow=self)

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
            flow=self,
        )
        create_duckdb_table.set_upstream(parquet, flow=self)
