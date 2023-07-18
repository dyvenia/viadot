from typing import Any, Dict, List, Literal

from prefect import Flow

from viadot.task_utils import df_to_parquet
from viadot.tasks import SQLServerToDF


class SQLServerToParquet(Flow):
    def __init__(
        self,
        name,
        sql_query: str,
        local_file_path: str,
        sqlserver_config_key: str = None,
        if_exists: Literal["fail", "replace", "append", "skip", "delete"] = "fail",
        if_empty: Literal["warn", "skip", "fail"] = "skip",
        timeout: int = 3600,
        *args: List[any],
        **kwargs: Dict[str, Any],
    ):
        """
        Flow for upolading data from SQL Server to Parquet file.

        Args:
            name (str): The name of the flow.
            sql_query (str, required): The query to execute on the SQL Server database. If don't start with "SELECT"
                returns empty DataFrame.
            local_file_path (str): Path to output parquet file.
            sqlserver_config_key (str, optional): The key inside local config containing the credentials. Defaults to None.
            if_exists (Literal, optional):  What to do if the table already exists. Defaults to "fail".
            if_empty (Literal, optional): What to do if Parquet file is empty. Defaults to "skip".
            timeout(int, optional): The amount of time (in seconds) to wait while running this task before
                a timeout occurs. Defaults to 3600.
        """
        # SQLServerToDF
        self.sql_query = sql_query
        self.sqlserver_config_key = sqlserver_config_key
        self.timeout = timeout

        # DuckDBCreateTableFromParquet
        self.local_file_path = local_file_path
        self.if_exists = if_exists
        self.if_empty = if_empty

        super().__init__(*args, name=name, **kwargs)

        self.gen_flow()

    def gen_flow(self) -> Flow:
        df_task = SQLServerToDF(timeout=self.timeout)
        df = df_task.bind(
            config_key=self.sqlserver_config_key, query=self.sql_query, flow=self
        )
        parquet = df_to_parquet.bind(
            df=df,
            path=self.local_file_path,
            if_exists=self.if_exists,
            flow=self,
        )
