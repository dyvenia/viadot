import pytest
import pandas as pd
from typing import List
from viadot.sources import Genesys
from viadot.tasks import GenesysToDF
from unittest import mock


@pytest.mark.init
def test_create_genesys_class():
    g = Genesys()
    assert g


@pytest.mark.init
def test_default_credential_param():
    g = Genesys()
    assert g.credentials != None and type(g.credentials) == dict


@pytest.mark.init
def test_environment_param():
    g = Genesys()
    assert g.environment != None and type(g.environment) == str


@pytest.mark.init
def test_schedule_id_param():
    g = Genesys()
    assert g.schedule_id != None and type(g.schedule_id) == str


@pytest.mark.report
def test_report_url_param():
    g = Genesys(schedule_id="9fb3a99e-aa5b-438b-9047-f4d7fe6d4ff3")
    test_url = g.get_analitics_url_report
    assert type(test_url) == str and test_url.startswith("http")


@pytest.mark.init
def test_report_clomuns_param():
    g = Genesys()
    assert g.report_columns != None and type(g.report_columns) == List


@pytest.mark.parametrize("input_name", ["", "test_name", "12345", ".##@@"])
@pytest.mark.init
def test_other_inicial_params(input_name):
    g = Genesys(report_name=input_name)
    assert len(g.report_name) > 0 and type(g.report_name) == str


@pytest.mark.get_all
def test_get_all_schedules_job():
    g = Genesys()
    assert type(g.get_all_schedules_job()) == dict


@pytest.mark.schedule_job
def test_schedule_rob():
    with mock.patch("viadot.sources.genesys.Genesys.schedule_report") as mock_method:
        mock_method.return_value = 200
        g = Genesys()

        data_to_post = {
            "name": "Schedule report job for test",
            "quartzCronExpression": "0 15 * * * ?",
            "description": "Export Test",
            "timeZone": "Europe/Warsaw",
            "timePeriod": "YESTERDAY",
            "interval": "2022-07-10T22:00:00.000Z/2022-07-11T22:00:00.000Z",
            "reportFormat": "XLS",
            "locale": "en_US",
            "enabled": True,
            "reportId": "03bc1eef-082e-46c1-b9a8-fe45fbc3b205",
            "parameters": {
                "queueIds": [
                    "780807e6-83b9-44be-aff0-a41c37fab004",
                ]
            },
        }

        status_code = g.schedule_report(data_to_post=data_to_post)
        assert status_code == 200
