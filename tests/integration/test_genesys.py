import os
import pytest
import pandas as pd
from typing import List
from unittest import mock

from viadot.sources import Genesys
from viadot.tasks import GenesysToDF


@pytest.fixture
def var_dictionary():
    variables = {
        "id": "9fb3a99e-aa5b-438b-9047-f4d7fe6d4ff3",
        "data_to_post": {
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
        },
        "url": "https://apps.mypurecloud.de/platform/api/v2/downloads/7dca3529280847af",
        "file_name": "Genesys_Queue_Metrics_Interval_Export.xls",
    }

    return variables


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


@pytest.mark.parametrize("input_name", ["test_name", "12345", ".##@@"])
@pytest.mark.init
def test_other_inicial_params(input_name):
    g = Genesys(report_name=input_name)
    assert len(g.report_name) > 0 and type(g.report_name) == str


@pytest.mark.proper
def test_connection_with_genesys_api():
    g = Genesys()
    test_genesys_connection = g.authorization_token
    assert (
        type(test_genesys_connection) == dict
        and len(test_genesys_connection.items()) > 0
    )


@pytest.mark.proper
def test_get_analitics_url_report(var_dictionary):
    g = Genesys(schedule_id=var_dictionary["id"])
    test_url = g.get_analitics_url_report
    assert type(test_url) == str and test_url.startswith("http")


@mock.patch.object(Genesys, "schedule_report")
@pytest.mark.connection
def test_scheduling_report(mock_api_response, var_dictionary):
    mock_api_response.return_value = 200

    g = Genesys()
    result = g.schedule_report(data_to_post=var_dictionary["data_to_post"])
    assert result == 200
    mock_api_response.assert_called()


@pytest.mark.dependency(name="url")
@pytest.mark.url
def test_get_all_schedules_job():
    g = Genesys()
    reports = g.get_all_schedules_job()
    assert (
        type(reports) == dict
        and len(reports.keys()) > 0
        and len(reports.get("entities")[0]["id"]) > 0
    )


@pytest.mark.dependency(depends=["url"])
@pytest.mark.url
def test_report_to_df(var_dictionary):
    g = Genesys(schedule_id=var_dictionary["id"])
    df = g.to_df()
    assert type(df) == pd.DataFrame and df.shape != (0, 0)


@pytest.mark.download
def test_download_report_file(var_dictionary):
    g = Genesys()
    g.download_report(report_url=var_dictionary["url"])
    assert os.path.exists(var_dictionary["file_name"]) == True

    if os.path.exists(var_dictionary["file_name"]):
        os.remove(var_dictionary["file_name"])


@pytest.mark.task
def test_genesys_to_df_task(var_dictionary):
    task = GenesysToDF(schedule_id=var_dictionary["id"])
    result = task.run()
    assert isinstance(result, pd.DataFrame)
