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
        is_date_range_type: bool = None,
        date: List[str] = None,
        *args,
        **kwargs,
    ):

        self.flow_name = flow_name
        self.is_date_range_type = is_date_range_type
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

    def calculate_difference(
        self,
        date_to_compare: datetime = None,
        base_date: datetime = datetime.today(),
        is_date_range_type: bool = None,
        diff_type: Literal["time", "date"] = "date",
    ):
        """Calculations for set new date are needed."""
        if is_date_range_type is True:
            if diff_type == "date":
                difference = base_date - date_to_compare
                return difference.days
            if diff_type == "time":
                difference_h = abs(base_date.hour - date_to_compare.hour)
                if difference_h >= 1:
                    return difference_h
                else:
                    return difference_h
        if is_date_range_type is False:
            return date_to_compare

    def check_if_scheduled_run(
        self, time_run: str = None, time_schedule: str = None
    ) -> bool:
        """Check if run was schduled or started by user"""
        diff = self.calculate_difference(
            date_to_compare=time_run,
            base_date=time_schedule,
            is_date_range_type=True,
            diff_type="time",
        )
        if diff < 1:
            return True
        if diff > 1:
            return False

    @defaults_from_attrs(
        "flow_name",
        "is_date_range_type",
        "date",
    )
    def run(
        self,
        flow_name,
        is_date_range_type,
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
                      scheduled_start_time
                    }  
                } 
            }
        """
            % flow_name
        )

        ## check if is scheduled

        flow_runs = client.graphql(query)
        flow_runs_ids = flow_runs.data.flow[0]["flow_runs"]

        last_success_start_time = self.check_fails(flow_runs_ids)
        time_schedule = flow_runs_ids[0]["scheduled_start_time"]

        is_scheduled = self.check_if_scheduled_run(
            time_run=last_success_start_time,
            time_schedule=time_schedule,
        )
        if is_scheduled is True:
            new_date = self.calculate_difference(
                date_to_compare=last_success_start_time,
                base_date=time_schedule,
                is_date_range_type=is_date_range_type,
                diff_type="date",
            )
        if is_scheduled is False:
            new_date = self.calculate_difference(
                date_to_compare=last_success_start_time,
                base_date=time_schedule,
                is_date_range_type=is_date_range_type,
                diff_type="date",
            )

        return new_date
