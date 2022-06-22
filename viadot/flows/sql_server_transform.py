from prefect import Flow, config
from typing import Any, Dict, List, Literal

from ..tasks import SQLServerQuery

query_task = SQLServerQuery()


class SQLServerTransform(Flow):
    def __init__(
        self,
        name: str,
        query: str,
        config_key: str,
        *args: List[any],
        **kwargs: Dict[str, Any]
    ):
        """
        Flow for running SQL queries on top of SQL Server.

        Args:
            name (str,required): The name of the flow.
            query (str, required): The query to execute on the database.
            config_key (str, required): Config key containing credentials for the SQL Server connection.
        """
        self.query = query
        self.config_key = config_key

        super().__init__(*args, name=name, **kwargs)
        self.gen_flow()

    def gen_flow(self) -> Flow:
        query_task.bind(
            query=self.query,
            config_key=self.config_key,
            flow=self,
        )
