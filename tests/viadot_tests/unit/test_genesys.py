from unittest import mock

import pandas as pd
import pytest

from viadot.exceptions import APIError
from viadot.sources import Genesys


@pytest.fixture(scope="function")
def var_dictionary():
    variables = {
        "data_to_post_0": [
            eval(
                """{
                "name": "QUEUE_PERFORMANCE_DETAIL_VIEW_unknown",
                "timeZone": "UTC",
                "exportFormat": "CSV",
                "interval": "2022-08-12T23:00:00/2022-08-13T23:00:00",
                "period": "PT30M",
                "viewType": f"QUEUE_PERFORMANCE_DETAIL_VIEW",
                "filter": {
                            "mediaTypes": ["unknown"], 
                            "queueIds": ["{'1234567890', '1234567890'}"], 
                            "directions":["inbound"],
                            },
                "read": True,
                "locale": "en-us",
                "hasFormatDurations": False,
                "hasSplitFilters": True,
                "excludeEmptyRows": True,
                "hasSplitByMedia": True,
                "hasSummaryRow": True,
                "csvDelimiter": "COMMA",
                "hasCustomParticipantAttributes": True,
                "recipientEmails": [],
            }"""
            )
        ],
        "report_data": [
            [
                "1234567890qwertyuiopasdfghjklazxcvbn",
                "https://apps.mypurecloud.de/example/url/test",
                "1234567890qwertyuiopasdfghjklazxcvbn",
                "chat",
                "QUEUE_PERFORMANCE_DETAIL_VIEW",
                "2022-08-12T23:00:00.000Z/2022-08-13T23:00:00.000Z",
                "COMPLETED",
            ],
            [
                "1234567890qwertyuiopasdfghjklazxcvbn",
                "https://apps.mypurecloud.de/example/url/test",
                "1234567890qwertyuiopasdfghjklazxcvbn",
                "chat",
                "QUEUE_PERFORMANCE_DETAIL_VIEW",
                "2022-08-12T23:00:00.000Z/2022-08-13T23:00:00.000Z",
                "COMPLETED",
            ],
            [
                "1234567890qwertyuiopasdfghjklazxcvbn",
                "https://apps.mypurecloud.de/example/url/test",
                "1234567890qwertyuiopasdfghjklazxcvbn",
                "callback",
                "QUEUE_PERFORMANCE_DETAIL_VIEW",
                "2022-08-12T23:00:00.000Z/2022-08-13T23:00:00.000Z",
                "COMPLETED",
            ],
            [
                "1234567890qwertyuiopasdfghjklazxcvbn",
                "https://apps.mypurecloud.de/example/url/test",
                "1234567890qwertyuiopasdfghjklazxcvbn",
                "callback",
                "QUEUE_PERFORMANCE_DETAIL_VIEW",
                "2022-08-12T23:00:00.000Z/2022-08-13T23:00:00.000Z",
                "COMPLETED",
            ],
        ],
        "entities": {
            "entities": [
                {
                    "id": "1234567890",
                    "name": "QUEUE_PERFORMANCE_DETAIL_VIEW_chat",
                    "runId": "1234567890",
                    "status": "COMPLETED",
                    "timeZone": "UTC",
                    "exportFormat": "CSV",
                    "interval": "2022-08-02T23:00:00.000Z/2022-08-03T23:00:00.000Z",
                    "downloadUrl": "https://apps.mypurecloud.de/example/url/test",
                    "viewType": "QUEUE_PERFORMANCE_DETAIL_VIEW",
                    "period": "PT30M",
                    "filter": {
                        "mediaTypes": ["chat"],
                        "queueIds": ["1234567890"],
                        "directions": ["inbound"],
                    },
                    "read": False,
                    "createdDateTime": "2022-08-03T11:19:47Z",
                    "modifiedDateTime": "2022-08-03T11:19:49Z",
                    "locale": "en-us",
                    "percentageComplete": 1.0,
                    "hasFormatDurations": False,
                    "hasSplitFilters": True,
                    "excludeEmptyRows": True,
                    "hasSplitByMedia": True,
                    "hasSummaryRow": True,
                    "csvDelimiter": "COMMA",
                    "hasCustomParticipantAttributes": True,
                    "dateLastConfirmed": "2022-08-03T11:19:47Z",
                    "intervalKeyType": "ConversationStart",
                    "enabled": False,
                    "selfUri": "/api/v2/example/url/test",
                },
            ],
            "pageSize": 100,
            "pageNumber": 1,
            "total": 6,
            "pageCount": 1,
        },
    }

    return variables


class MockClassHandle:
    status_code = 200

    def json():
        test = {"token_type": None, "access_token": None}
        return test


@mock.patch("viadot.sources.genesys.handle_api_response", return_value=MockClassHandle)
@pytest.mark.connect
def test_connection_genesys_api(mock_connection):
    g = Genesys(
        config_key="genesys",
        verbose=True,
    )
    test_genesys_connection = g.authorization_token
    assert (
        isinstance(test_genesys_connection, dict)
        and len(test_genesys_connection.items()) > 0
    )
    mock_connection.assert_called_once()


@mock.patch(
    "viadot.sources.genesys.Genesys._api_call",
    return_value=MockClassHandle,
)
@mock.patch(
    "viadot.sources.genesys.Genesys._load_reporting_exports",
    return_value={"entities": [{}]},
)
@mock.patch(
    "viadot.sources.genesys.Genesys._get_reporting_exports_url",
    return_value=([0], ["https://apps.mypurecloud.de/example/url/test"]),
)
@mock.patch(
    "viadot.sources.genesys.Genesys._download_report", return_value=pd.DataFrame()
)
@mock.patch("viadot.sources.genesys.Genesys._delete_report", return_value=True)
@pytest.mark.connect
def test_genesys_api_response(
    mock_connection0,
    mock_connection1,
    mock_connection2,
    mock_connection3,
    mock_connection4,
    var_dictionary,
):
    g = Genesys(
        config_key="genesys",
        verbose=True,
    )
    g.api_connection(
        endpoint="analytics/reporting/exports",
        view_type="agent_status_summary_view",
        view_type_time_sleep=5,
        post_data_list=var_dictionary["data_to_post_0"],
    )
    mock_connection0.assert_called_once()
    mock_connection1.assert_called_once()
    mock_connection2.assert_called_once()
    mock_connection3.assert_called_once()
    mock_connection4.assert_called_once()


@mock.patch(
    "viadot.sources.genesys.Genesys._api_call",
    return_value=MockClassHandle,
)
@mock.patch(
    "viadot.sources.genesys.Genesys._load_reporting_exports",
    return_value={"entities": [{}]},
)
@mock.patch(
    "viadot.sources.genesys.Genesys._get_reporting_exports_url",
    return_value=([0], [None]),
)
@mock.patch(
    "viadot.sources.genesys.Genesys._download_report", return_value=pd.DataFrame()
)
@mock.patch("viadot.sources.genesys.Genesys._delete_report", return_value=True)
@pytest.mark.connect
def test_genesys_api_error(
    mock_connection0,
    mock_connection1,
    mock_connection2,
    mock_connection3,
    mock_connection4,
    var_dictionary,
):
    g = Genesys(
        config_key="genesys",
        verbose=True,
    )
    with pytest.raises(APIError):
        g.api_connection(
            endpoint="analytics/reporting/exports",
            view_type="agent_status_summary_view",
            view_type_time_sleep=5,
            post_data_list=var_dictionary["data_to_post_0"],
        )

    mock_connection0.assert_called_once()
    assert not mock_connection1.called
    mock_connection2.assert_called_once()
    mock_connection3.assert_called_once()
    mock_connection4.assert_called_once()


@mock.patch(
    "viadot.sources.genesys.Genesys._api_call",
    return_value=MockClassHandle,
)
@mock.patch(
    "viadot.sources.genesys.Genesys._load_reporting_exports",
    return_value={"entities": [{}]},
)
@mock.patch(
    "viadot.sources.genesys.Genesys._get_reporting_exports_url",
    return_value=([0], ["https://apps.mypurecloud.de/example/url/test"]),
)
@mock.patch(
    "viadot.sources.genesys.Genesys._download_report", return_value=pd.DataFrame()
)
@mock.patch("viadot.sources.genesys.Genesys._delete_report", return_value=True)
@pytest.mark.response
def test_genesys_api_df_response(
    mock_connection0,
    mock_connection1,
    mock_connection2,
    mock_connection3,
    mock_connection4,
    var_dictionary,
):
    g = Genesys(
        config_key="genesys",
        verbose=True,
    )
    g.api_connection(
        endpoint="analytics/reporting/exports",
        view_type="agent_status_summary_view",
        view_type_time_sleep=5,
        post_data_list=var_dictionary["data_to_post_0"],
    )

    df = g.to_df()
    viadot_set = {"_viadot_source", "_viadot_downloaded_at_utc"}

    assert set(df.columns).issuperset(viadot_set)
    assert isinstance(df, pd.DataFrame)
