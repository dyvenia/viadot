from os import times_result
from typing import List, Literal
import prefect
from datetime import date, datetime
import pandas as pd

from prefect import Task
from prefect.utilities.tasks import defaults_from_attrs
from prefect.utilities import logging

logger = logging.get_logger()


class GetFlowLastSuccessfulRun(Task):
    def __init__(
        self,
        flow_name: str = None,
        date_range_type: bool = None,
        *args,
        **kwargs,
    ):

        self.flow_name = flow_name
        self.date_range_type = date_range_type

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

    def get_time_from_last_successful_run(self, flow_run_ids: str = None):
        """
        Get start_time from last Flow run where state was success
        """
        for flow_run in self.iter_throught_flow_runs_ids(flow_run_ids):
            if flow_run.state == "Success":
                return flow_run.start_time

    def calculate_difference(
        self,
        date_to_compare: str = None,
        base_date: str = str(datetime.today()),
        diff_type: Literal["time", "date"] = "date",
    ):
        """Calculations for set new date are needed."""
        base_date = self.get_formatted_date(base_date, diff_type)
        date_to_compare = self.get_formatted_date(date_to_compare, diff_type)

        if diff_type == "date":
            difference = abs(base_date - date_to_compare)
            return difference.days

        if diff_type == "time":
            difference_h = abs(base_date.hour - date_to_compare.hour)
            if difference_h <= 1:
                difference_m = date_to_compare.minute - base_date.minute
                if difference_m <= 0:
                    return 1
                if difference_m > 0:
                    return float(f"1.{(abs(difference_m))}")
            if difference_h > 1:
                return difference_h

    def check_if_scheduled_run(
        self, time_run: str = None, time_schedule: str = None
    ) -> bool:
        """Check if run was schduled or started by user"""
        diff = self.calculate_difference(
            date_to_compare=time_run,
            base_date=time_schedule,
            diff_type="time",
        )
        if diff < 1:
            return True
        if diff >= 1:
            return False

    def get_formatted_date(
        self,
        time_unclean: str = None,
        return_value: Literal["time", "date"] = "date",
    ):
        """
        from prefect format date (in string) get clean time or date in datetime type.
        - date from Prefect: '2022-02-21T01:00:00+00:00'
        """
        if return_value == "time":
            time_extracted = time_unclean.split("T")[1]
            time_clean_str = time_extracted.split(".")[0]
            time_clean = datetime.strptime(time_clean_str[:8], "%H:%M:%S")
            return time_clean.time()

        if return_value == "date":
            date_extracted = time_unclean.split("T")[0]
            date_clean = datetime.strptime(date_extracted, "%Y-%m-%d")
            return date_clean.date()

    @defaults_from_attrs(
        "flow_name",
        "date_range_type",
    )
    def run(
        self,
        flow_name,
        date_range_type,
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
                      scheduled_start_time
                    }  
                } 
            }
        """
            % flow_name
        )

        flow_runs = client.graphql(query)
        flow_runs_ids = flow_runs.data.flow[0]["flow_runs"]

        last_success_start_time = self.get_time_from_last_successful_run(flow_runs_ids)
        time_schedule = flow_runs_ids[0]["scheduled_start_time"]
        is_scheduled = self.check_if_scheduled_run(
            time_run=last_success_start_time,
            time_schedule=time_schedule,
        )
        if is_scheduled is True:
            new_date = self.calculate_difference(
                date_to_compare=last_success_start_time,
                base_date=time_schedule,
                diff_type="date",
            )
        if is_scheduled is False:
            return self.date_range_type

        return new_date
