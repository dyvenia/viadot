from datetime import datetime
from typing import List, Literal

import prefect
from prefect import Task
from prefect.utilities import logging
from prefect.utilities.tasks import defaults_from_attrs

logger = logging.get_logger()


def iter_throught_flow_runs(flow_runs_details: List[dict] = None) -> dict:
    """
    Generate Flow run details from dict containing flow runs

    Args:
        flow_runs_details (List[dict], optional): List of Flow run details. Defaults to None.

    Yields:
        dict: Flow run details
    """
    for x in range(len(flow_runs_details)):
        for flow_run in flow_runs_details[x]["flow_runs"]:
            yield flow_run


def get_time_from_last_successful_run(flow_runs_details: List[dict] = None) -> str:
    """
    Get start_time from last flow run where state was success.

    Args:
        flow_runs_details (List[dict], optional): List of Flow run details. Defaults to None.

    Returns:
        str: Flow run start_time
    """

    for flow_run in iter_throught_flow_runs(flow_runs_details=flow_runs_details):
        if flow_run["state"] == "Success":
            return flow_run["start_time"]


def calculate_difference(
    date_to_compare: str = None,
    base_date: str = str(datetime.today()),
    diff_type: Literal["time", "date"] = "date",
):
    """
    Calculate diffrence between two dates.

    Args:
        date_to_compare (str, optional): Date to compare with base_date (Flow run start_time). Defaults to None.
        base_date (str, optional): The base date (be saved as Prefect schedule date. Defaults to str(datetime.today()).
        diff_type (Literal["time", "date"], optional): _description_. Defaults to "date".

     Returns:
        (int, float): Differences in days when calculating date or time (hours, minutes) when calculating time.
    """
    base_date = get_formatted_date(base_date, diff_type)
    date_to_compare = get_formatted_date(date_to_compare, diff_type)

    if diff_type == "date":
        difference = abs(base_date - date_to_compare)
        return difference.days

    if diff_type == "time":
        difference_h = abs(base_date.hour - date_to_compare.hour)
        difference_m = date_to_compare.minute - base_date.minute
        if difference_h == 1:
            if difference_m < 0:
                return 0
            if difference_m > 0:
                return float(f"1.{(abs(difference_m))}")
            if difference_m == 0:
                return 1
        if difference_h < 1:
            return 0
        if difference_h > 1:
            return difference_h


def check_if_scheduled_run(time_run: str = None, time_schedule: str = None) -> bool:
    """
    Check if run was scheduled or started by user.

    Args:
        time_run (str, optional): The time the Flow was started. Defaults to None.
        time_schedule (str, optional): Scheduled time of Flow. Defaults to None.

    Returns:
        bool: True if flow run was started automatically. False if Flow was started by user.
    """
    diff = calculate_difference(
        date_to_compare=time_run,
        base_date=time_schedule,
        diff_type="time",
    )
    if diff <= 1:
        return True
    if diff > 1:
        return False


def get_formatted_date(
    time_unclean: str = None,
    return_value: Literal["time", "date"] = "date",
):
    """
    Format date from "2022-02-21T01:00:00+00:00" to date or time.

    Args:
        time_unclean (str, optional): Time in datetime format obtained from Prefect. Defaults to None.
        return_value (Literal["time", "date"], optional): Choose the format to be extracted from datetime - time or date.
                                                        Defaults to "date".

    Returns:
        datetime: Date (datetime.date) or time (datetime.time)
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


class GetFlowNewDateRange(Task):
    """
    Get the flow date_range_type and add the days if the last run of the flow was unsuccessful.
    A task that checks the status of the last flow run and change it if the last flow has failed.

    If it is originally 'last_14_days' but the flow has run in Failed state for the last 2 days
    then from 'last_14_days' it will be 'last_16_days' in the next flow_run.

    Note that here is the check whether the flow run has been planned.
    Only If the flow was run by a prefect (according to the planned schedule), a change of date dates is possible.
    """

    def __init__(
        self,
        flow_name: str = None,
        date_range_type: str = None,
        timeout: int = 3600,
        *args,
        **kwargs,
    ):
        """
        Initialize GetFlowNewDateRange class.

        Args:
            flow_name (str, optional): Prefect flow name. Defaults to None.
            date_range_type (str, optional): String argument that should look like this: 'last_X_days' -
                X is a number of days. Defaults to None.
            timeout(int, optional): The amount of time (in seconds) to wait while running this task before
                a timeout occurs. Defaults to 3600.
        """

        self.flow_name = flow_name
        self.date_range_type = date_range_type

        super().__init__(
            name="prefect_extract_details",
            timeout=timeout,
            *args,
            **kwargs,
        )

    def __call__(self):
        """Extract time from Prefect Flow run"""
        super().__call__(self)

    def change_date_range(self, date_range: str = None, difference: int = None) -> str:
        """
        Replace number of days in string "last_X_days" based on the sum of number extracted from string
        and difference passed to function.

        Args:
            date_range (str, optional): Date range in format "last_X_days". Defaults to None.
            difference (int, optional): Int value representing days. Defaults to None.

        Returns:
            str: Date range in "last_X_days" format
        """
        old_range_splitted = date_range.split("_")
        old_range = int(old_range_splitted[1])
        new_range = old_range + difference

        new_range_splitted = old_range_splitted
        new_range_splitted[1] = str(new_range)
        date_range_type = "_".join(new_range_splitted)
        return date_range_type

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

        client = prefect.Client()
        flow_runs = client.graphql(query)
        flow_runs_details = flow_runs.data.flow

        for flow_run in iter_throught_flow_runs(flow_runs_details=flow_runs_details):
            if flow_run["scheduled_start_time"]:
                time_schedule = flow_run["scheduled_start_time"]
                break
        last_success_start_time = get_time_from_last_successful_run(flow_runs_details)
        is_scheduled = check_if_scheduled_run(
            time_run=last_success_start_time,
            time_schedule=time_schedule,
        )

        if is_scheduled is True:
            difference_days = calculate_difference(
                date_to_compare=last_success_start_time,
                base_date=time_schedule,
                diff_type="date",
            )
            new_date_range_type = self.change_date_range(
                date_range=date_range_type, difference=difference_days
            )
            return new_date_range_type

        if is_scheduled is False:
            return date_range_type
