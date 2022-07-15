import datetime
from datetime import date

import pandas as pd
import pytest

from viadot.tasks import GetFlowNewDateRange
from viadot.tasks.prefect_date_range import (
    calculate_difference,
    check_if_scheduled_run,
    get_formatted_date,
    get_time_from_last_successful_run,
)

PREFECT_TASK = GetFlowNewDateRange()
DATE_FROM_PREFECT = "2022-01-01T01:30:00+00:00"
DATE_FROM_PREFECT2 = "2022-01-04T02:20:00+00:00"
PREFECT_JSON = {
    "data": {
        "flow": [
            {
                "flow_runs": [
                    {
                        "id": "88a505ec-f7c9-4242-baa4-81c9db1aaafe",
                        "start_time": "2022-02-21T01:04:44.146191+00:00",
                        "state": "Failed",
                        "scheduled_start_time": "2022-02-21T01:00:00+00:00",
                    },
                    {
                        "id": "9d310a47-2fb0-4435-971c-4fcaf3efca06",
                        "start_time": "2022-02-20T01:05:36.142547+00:00",
                        "state": "Success",
                        "scheduled_start_time": "2022-02-20T01:00:00+00:00",
                    },
                    {
                        "id": "90c32b13-4a0a-467b-a07f-c568a34f1bc2",
                        "start_time": "2022-02-19T01:05:30.572431+00:00",
                        "state": "Failed",
                        "scheduled_start_time": "2022-02-19T01:00:00+00:00",
                    },
                ]
            }
        ]
    }
}


def test_get_formatted_dated():
    new_date = get_formatted_date(time_unclean=DATE_FROM_PREFECT, return_value="date")
    assert new_date == datetime.date(2022, 1, 1)
    assert isinstance(new_date, date)

    new_time = get_formatted_date(time_unclean=DATE_FROM_PREFECT, return_value="time")
    assert new_time == datetime.time(1, 30)
    assert isinstance(new_time, datetime.time)


def test_calculate_difference_date():
    diff_days = calculate_difference(
        date_to_compare=DATE_FROM_PREFECT2,
        base_date=DATE_FROM_PREFECT,
        diff_type="date",
    )
    assert diff_days == 3


def test_calculate_difference_time():
    diff_hours = calculate_difference(
        date_to_compare=DATE_FROM_PREFECT2,
        base_date=DATE_FROM_PREFECT,
        diff_type="time",
    )
    assert diff_hours == 0

    diff_hours = calculate_difference(
        date_to_compare="2022-01-04T02:50:00+00:00",
        base_date=DATE_FROM_PREFECT,
        diff_type="time",
    )
    assert diff_hours == 1.20


def test_get_time_from_last_successful_run():
    flow_runs = PREFECT_JSON["data"]["flow"]
    start_time_success = get_time_from_last_successful_run(flow_runs_details=flow_runs)
    assert start_time_success == "2022-02-20T01:05:36.142547+00:00"


def test_check_if_scheduled_run():
    is_scheduled = check_if_scheduled_run(
        time_run="2022-02-21T01:40:00+00:00", time_schedule="2022-02-15T01:00:00+00:00"
    )
    assert is_scheduled is True

    is_scheduled = check_if_scheduled_run(
        time_run="2022-02-21T02:20:00+00:00", time_schedule="2022-02-15T01:00:00+00:00"
    )
    assert is_scheduled is False


def test_change_date_range():
    date_range = "last_5_days"
    difference = 10
    date_range_new = PREFECT_TASK.change_date_range(
        date_range=date_range, difference=difference
    )
    assert date_range_new == "last_15_days"
