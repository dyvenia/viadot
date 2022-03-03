from typing import Any, Dict, List

from prefect import Flow

from ..tasks.duckdb import DuckDBQuery

query_task = DuckDBQuery()


class DuckDBTransform(Flow):
    def __init__(
        self,
        name: str,
        query: str,
        credentials: dict = None,
        tags: List[str] = ["transform"],
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
        """
        self.query = query
        self.credentials = credentials
        self.tags = tags
        self.tasks = [query_task]

        super().__init__(*args, name=name, **kwargs)
        self.gen_flow()

    def gen_flow(self) -> Flow:
        query_task.bind(
            query=self.query,
            credentials=self.credentials,
            flow=self,
        )
