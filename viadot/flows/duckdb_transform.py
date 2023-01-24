from typing import Any, Dict, List

from prefect import Flow

from viadot.tasks.duckdb import DuckDBQuery


class DuckDBTransform(Flow):
    def __init__(
        self,
        name: str,
        query: str,
        credentials: dict = None,
        tags: List[str] = ["transform"],
        timeout: int = 3600,
        *args: List[any],
        **kwargs: Dict[str, Any]
    ):
        """
        Flow for running SQL queries on top of DuckDB.

        Args:
            name (str): The name of the flow.
            query (str, required): The query to execute on the database.
            credentials (dict, optional): Credentials for the connection. Defaults to None.
            tags (list, optional): Tag for marking flow. Defaults to "transform".
            timeout(int, optional): The amount of time (in seconds) to wait while running this task before
                a timeout occurs. Defaults to 3600.
        """
        self.query = query
        self.credentials = credentials
        self.tags = tags
        self.timeout = timeout

        super().__init__(*args, name=name, **kwargs)
        self.gen_flow()

    def gen_flow(self) -> Flow:
        query_task = DuckDBQuery(timeout=self.timeout)
        query_task.bind(
            query=self.query,
            credentials=self.credentials,
            flow=self,
        )
