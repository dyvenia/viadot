from typing import Any, Dict, List

from prefect import Flow
from prefect.utilities import logging

from ..tasks.azure_sql import RunAzureSQLDBQuery

logger = logging.get_logger(__name__)

query_task = RunAzureSQLDBQuery()


class AzureSQLTransform(Flow):
    def __init__(
        self, name: str, query: str, *args: List[any], **kwargs: Dict[str, Any]
    ):
        self.query = query
        self.tasks = [query_task]
        super().__init__(*args, name=name, **kwargs)
        self.gen_flow()

    def gen_flow(self) -> Flow:
        query_task.bind(query=self.query, flow=self)
