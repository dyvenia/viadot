from typing import List
import prefect
from datetime import date, datetime
import pandas as pd

from prefect import Task
from prefect.utilities.tasks import defaults_from_attrs
from prefect.utilities import logging

logger = logging.get_logger()


class PrefectExtract(Task):
    def __init__(
        self,
        flow_name: str = None,
        if_date_range_type: bool = None,
        date: List[str] = None,
        *args,
        **kwargs,
    ):

        self.flow_name = flow_name
        self.if_date_range_type = if_date_range_type
        self.date = date

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
        for id in range(len(run_ids_list)):
            yield run_ids_list[id]

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

    @defaults_from_attrs(
        "flow_name",
        "if_date_range_type",
        "date",
    )
    def run(
        self,
        flow_name,
        if_date_range_type,
        date,
        **kwargs,
    ) -> None:

        client = prefect.Client()

        query = (
            """
             {           
                flow (where: { name: { _eq: "%s" } } )
                {
                flow_runs(
                    order_by: {end_time: desc}
                    where: {start_time:{ _is_null:false } } ) 
                    {
                      id
                      end_time
                      start_time
                      state
                    }  
                } 
            }
        """
            % flow_name
        )

        flow_runs = client.graphql(query)
        flow_runs_ids = flow_runs.data.flow[0]["flow_runs"]

        last_success = self.check_fails(flow_runs_ids)
        if if_date_range_type is True:
            print(self.if_date_range_type)

        new_date = self.format_date(last_success)[0]
        new_last_days = self.format_date(last_success)[1]

        return [new_date, new_last_days]
