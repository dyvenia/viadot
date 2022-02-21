import pytest
import pandas as pd
import datetime
from datetime import date

from viadot.tasks import GetFlowLastSuccessfulRun

PREFECT_TASK = GetFlowLastSuccessfulRun()
DATE_FROM_PREFECT = "2022-01-01T01:30:00+00:00"
DATE_FROM_PREFECT2 = "2022-01-04T02:20:00+00:00"


def test_get_formatted_dated():
    new_date = PREFECT_TASK.get_formatted_date(
        time_unclean=DATE_FROM_PREFECT, return_value="date"
    )
    assert new_date == datetime.date(2022, 1, 1)
    assert isinstance(new_date, date)

    new_time = PREFECT_TASK.get_formatted_date(
        time_unclean=DATE_FROM_PREFECT, return_value="time"
    )
    assert new_time == datetime.time(1, 30)
    assert isinstance(new_time, datetime.time)


def test_calculate_difference_date():
    diff_days = PREFECT_TASK.calculate_difference(
        date_to_compare=DATE_FROM_PREFECT2,
        base_date=DATE_FROM_PREFECT,
        diff_type="date",
    )
    assert diff_days == 3


def test_calculate_difference_time():
    diff_hours = PREFECT_TASK.calculate_difference(
        date_to_compare=DATE_FROM_PREFECT2,
        base_date=DATE_FROM_PREFECT,
        diff_type="time",
    )
    assert diff_hours == 1

    diff_hours = PREFECT_TASK.calculate_difference(
        date_to_compare="2022-01-04T02:50:00+00:00",
        base_date=DATE_FROM_PREFECT,
        diff_type="time",
    )
    assert diff_hours == 1.20
