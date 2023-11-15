import logging
from unittest import mock

import pytest

from viadot.sources import Genesys


@pytest.fixture
def var_dictionary():
    variables = {
        "start_date": "2022-08-12",
        "media_type_list": ["callback", "chat"],
        "queueIds_list": [
            "1234567890",
            "1234567890",
        ],
        "data_to_post": """{
                "name": f"QUEUE_PERFORMANCE_DETAIL_VIEW_{media}",
                "timeZone": "UTC",
                "exportFormat": "CSV",
                "interval": f"{end_date}T23:00:00/{start_date}T23:00:00",
                "period": "PT30M",
                "viewType": f"QUEUE_PERFORMANCE_DETAIL_VIEW",
                "filter": {"mediaTypes": [f"{media}"], "queueIds": [f"{queueid}"], "directions":["inbound"],},
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
            }""",
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
        "ids_mapping": {"1234567890qwertyuiopasdfghjklazxcvbn": "TEST"},
    }

    return variables


class MockClass:
    status_code = 200

    def json():
        test = {"token_type": None, "access_token": None}
        return test


@pytest.mark.init
def test_create_genesys_class():
    g = Genesys()
    assert g


@pytest.mark.init
def test_default_credential_param():
    g = Genesys()
    assert g.credentials != None and type(g.credentials) == dict


@pytest.mark.init
def test_default_credentials_provided(caplog):
    with caplog.at_level(logging.INFO):
        Genesys(credentials_genesys={"CREDENTIALS_KEY": "value"})
    assert "Credentials provided by user" in caplog.text


@pytest.mark.init
def test_environment_param():
    g = Genesys()
    assert g.environment != None and type(g.environment) == str


@pytest.mark.parametrize("input_name", ["test_name", "12345", ".##@@"])
@pytest.mark.init
def test_other_inicial_params(input_name):
    g = Genesys(report_name=input_name)
    assert len(g.report_name) > 0 and isinstance(g.report_name, str)


@pytest.mark.proper
def test_connection_with_genesys_api():
    g = Genesys()
    test_genesys_connection = g.authorization_token
    assert (
        type(test_genesys_connection) == dict
        and len(test_genesys_connection.items()) > 0
    )


@mock.patch.object(Genesys, "genesys_api_connection")
@pytest.mark.connection
def test_generate_api_connection(mock_api_response, var_dictionary):
    g = Genesys()
    assert g.genesys_api_connection()
    mock_api_response.assert_called()


def test_api_connection_return_type():
    conn_dict = Genesys().genesys_api_connection(post_data_list=["test_value_to_post"])
    assert isinstance(conn_dict, dict)


def test_load_reporting_exports_return_type(caplog):
    with caplog.at_level(logging.INFO):
        load_return = Genesys().load_reporting_exports()
    assert isinstance(load_return, dict)

    assert "loaded" in caplog.text


@mock.patch.object(Genesys, "download_report")
@pytest.mark.dependency(depends=["test_generate_api_connection"])
@pytest.mark.download
def test_download_reports(mock_download_files, var_dictionary, caplog):
    g = Genesys()
    g.ids_mapping = var_dictionary["ids_mapping"]
    g.report_data = var_dictionary["report_data"]
    g.start_date = var_dictionary["start_date"]
    with caplog.at_level(logging.INFO):
        file_name_list = g.download_all_reporting_exports()
    assert "IDS_MAPPING loaded" in caplog.text

    g.ids_mapping = None
    with caplog.at_level(logging.WARNING):
        file_name_list = g.download_all_reporting_exports()
    assert "IDS_MAPPING is not provided" in caplog.text

    assert type(file_name_list) == list and len(file_name_list) > 0
    mock_download_files.assert_called()


@mock.patch("viadot.sources.genesys.handle_api_response", return_value=MockClass)
@pytest.mark.dependency(
    depends=[
        "test_generate_api_connection",
        "test_download_reports",
    ]
)
@pytest.mark.delete
def test_genesys_delete_reports(mock_api_response, var_dictionary):
    g = Genesys()
    g.report_data = var_dictionary["report_data"]
    g.delete_all_reporting_exports()
    mock_api_response.assert_called()
