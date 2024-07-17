from unittest import mock

import pandas as pd
import pytest

from viadot.exceptions import APIError
from viadot.sources import Genesys
from viadot.sources.genesys import GenesysCredentials


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


class TestGenesysCredentials:
    @pytest.fixture(scope="function")
    def genesyscredentials(self):
        return GenesysCredentials()


class TestGenesys:
    @pytest.fixture(scope="function")
    def genesys(self):
        return Genesys()

    def test_authorization_token(self, genesys):
        # TODO [use mock.patch, assert]: Implement test for Genesys.authorization_token (line 172)
        pass

    def test__api_call(self, genesys):
        # TODO [use mock.patch, assert]: Implement test for Genesys._api_call (line 210)
        pass

    def test__load_reporting_exports(self, genesys):
        # TODO [use mock.patch, assert]: Implement test for Genesys._load_reporting_exports (line 295)
        pass

    def test__get_reporting_exports_url(self, genesys):
        # TODO [use mock.patch, assert]: Implement test for Genesys._get_reporting_exports_url (line 330)
        pass

    def test__delete_report(self, genesys):
        # TODO [use mock.patch, assert]: Implement test for Genesys._delete_report (line 369)
        pass

    def test__download_report(self, genesys):
        # TODO [use mock.patch, assert]: Implement test for Genesys._download_report (line 392)
        pass

    def test__merge_conversations(self, genesys):
        # TODO [use mock.patch, assert]: Implement test for Genesys._merge_conversations (line 429)
        pass

    def test_api_connection(self, genesys):
        # TODO [use mock.patch, assert]: Implement test for Genesys.api_connection (line 633)
        pass

    def test_to_df(self, genesys):
        # TODO [use mock.patch, assert]: Implement test for Genesys.to_df (line 845)
        pass
