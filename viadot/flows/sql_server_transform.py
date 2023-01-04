from prefect import Flow, config
from typing import Any, Dict, List, Literal

from viadot.tasks import SQLServerQuery


class SQLServerTransform(Flow):
    def __init__(
        self,
        name: str,
        query: str,
        config_key: str,
        timeout: int = 3600,
        *args: List[any],
        **kwargs: Dict[str, Any]
    ):
        """
        Flow for running SQL queries on top of SQL Server.

        Args:
            name (str,required): The name of the flow.
            query (str, required): The query to execute on the database.
            config_key (str, required): Config key containing credentials for the SQL Server connection.
            timeout(int, optional): The amount of time (in seconds) to wait while running this task before
                a timeout occurs. Defaults to 3600.
        """
        self.query = query
        self.config_key = config_key
        self.timeout = timeout

        super().__init__(*args, name=name, **kwargs)
        self.gen_flow()

    def gen_flow(self) -> Flow:
        query_task = SQLServerQuery(timeout=self.timeout)
        query_task.bind(
            query=self.query,
            config_key=self.config_key,
            flow=self,
        )
