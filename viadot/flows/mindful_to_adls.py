from typing import Any, Dict, List

from datetime import datetime
from prefect import Flow

from viadot.tasks import MindfulToCSV


class GenesysToADLS(Flow):
    def __init__(
        self,
        name: str,
        credentials_mindful: Dict[str, Any] = None,
        start_date: datetime = None,
        end_date: datetime = None,
        date_interval: int = 1,
        *args: List[any],
        **kwargs: Dict[str, Any]
    ):

        self.credentials_mindful = credentials_mindful
        self.start_date = start_date
        self.end_date = end_date
        self.date_interval = date_interval

        super().__init__(*args, name=name, **kwargs)

        self.mind_flow()

    def mind_flow(self) -> Flow:
        to_csv = MindfulToCSV()

        to_csv.bind(
            credentials_mindful=self.credentials_mindful,
            start_date=self.start_date,
            end_date=self.end_date,
            date_interval=self.date_interval,
            flow=self,
        )
