from typing import List
import prefect
from datetime import date, datetime
import pandas as pd

from prefect import Task
from prefect.utilities.tasks import defaults_from_attrs

logger = logging.get_logger()


class PrefectExtract(Task):
    def __init__(
        self,
        *args,
        **kwargs,
    ):

        super().__init__(
            name="prefect_extract_details",
            *args,
            **kwargs,
        )

    def __call__(self):
        """Extract details from Prefect Flow"""
        super().__call__(self)

    def iter_throught_flow_runs_ids(self, run_ids_list: List[str] = None):
        """
        Generate Flow run ids
        """
        for ind in range(len(run_ids_list)):
            yield run_ids_list[ind]

    def check_fails(self, flow_run_ids: str = None):
        """
        Get start_time from last Flow run where state was success
        """
        for flow_run in self.iter_throught_flow_runs_ids(flow_run_ids):
            if flow_run.state == "Success":
                return flow_run.start_time

    def format_date(self, last_success: str = None):
        """
        Split date to date and time. Calculations for set new date are needed.
        """
        today = date.today()
        full_date_success = last_success.split("T")
        date_success = full_date_success[0]
        time_success = full_date_success[1].split(".")[0]
        return [date_success, time_success]

    @defaults_from_attrs()
    def run(
        self,
        **kwargs,
    ) -> None:
        pass
