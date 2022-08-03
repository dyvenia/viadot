import os
import pytest
import pandas as pd

from unittest import mock

from viadot.sources import Genesys

os.system("clear")


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
        "media_type_list": ["callback", "chat"],
        "queueIds_list": [
            "780807e6-83b9-44be-aff0-a41c37fab004",
            "383ee5e5-5d8f-406c-ad53-835b69fe82c5",
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
                "8629ef4c-7cc8-47cf-adf4-2424ca331796",
                "https://apps.mypurecloud.de/platform/api/v2/downloads/4e90fd3d6cbae494",
                "383ee5e5-5d8f-406c-ad53-835b69fe82c5",
                "chat",
            ],
            [
                "fb460d26-19a5-4115-82a8-c84e5fbb8652",
                "https://apps.mypurecloud.de/platform/api/v2/downloads/a9ffcb9f41dcc153",
                "780807e6-83b9-44be-aff0-a41c37fab004",
                "chat",
            ],
            [
                "ad162f8d-7768-4d95-bc7e-2ae2b69f8904",
                "https://apps.mypurecloud.de/platform/api/v2/downloads/eaf0564f25be892f",
                "383ee5e5-5d8f-406c-ad53-835b69fe82c5",
                "callback",
            ],
            [
                "2adba20f-8a06-44b4-973f-0cbb30450f22",
                "https://apps.mypurecloud.de/platform/api/v2/downloads/2c148bdd4a461415",
                "780807e6-83b9-44be-aff0-a41c37fab004",
                "callback",
            ],
        ],
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


@pytest.mark.dependency()
@pytest.mark.reports
def test_generate_body(var_dictionary):
    g = Genesys(
        media_type_list=var_dictionary["media_type_list"],
        queueIds_list=var_dictionary["queueIds_list"],
        data_to_post_str=var_dictionary["data_to_post"],
    )
    data_list = g.genesys_generate_body()
    assert type(data_list) == list


@mock.patch.object(Genesys, "genesys_generate_exports")
@pytest.mark.dependency(depends=["test_generate_body"])
@pytest.mark.connection
def test_generate_exports(mock_api_response, var_dictionary):
    g = Genesys(
        media_type_list=var_dictionary["media_type_list"],
        queueIds_list=var_dictionary["queueIds_list"],
        data_to_post_str=var_dictionary["data_to_post"],
    )
    assert g.genesys_generate_exports()
    mock_api_response.assert_called()


@mock.patch.object(Genesys, "load_reporting_exports")
@pytest.mark.dependency(depends=["test_generate_body", "test_generate_exports"])
@pytest.mark.generate
def test_generate_reports_list(mock_load_reports):
    mock_load_reports.return_value = {
        "entities": [
            {
                "id": "9d586b1e-bc48-445b-831c-e0967e537214",
                "name": "QUEUE_PERFORMANCE_DETAIL_VIEW_chat",
                "runId": "6119a10b-b9d9-4d17-bd30-b27a04076306",
                "status": "COMPLETED",
                "timeZone": "UTC",
                "exportFormat": "CSV",
                "interval": "2022-08-02T23:00:00.000Z/2022-08-03T23:00:00.000Z",
                "downloadUrl": "https://apps.mypurecloud.de/platform/api/v2/downloads/802026ef27d403b4",
                "viewType": "QUEUE_PERFORMANCE_DETAIL_VIEW",
                "period": "PT30M",
                "filter": {
                    "mediaTypes": ["chat"],
                    "queueIds": ["383ee5e5-5d8f-406c-ad53-835b69fe82c5"],
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
                "selfUri": "/api/v2/analytics/reporting/exports/9d586b1e-bc48-445b-831c-e0967e537214",
            },
            {
                "id": "30173782-7f79-40d3-9e28-0c80cee6569c",
                "name": "QUEUE_PERFORMANCE_DETAIL_VIEW_chat",
                "runId": "6bdf4a6c-33bc-4c47-bd0e-41e036397d25",
                "status": "COMPLETED",
                "timeZone": "UTC",
                "exportFormat": "CSV",
                "interval": "2022-08-02T23:00:00.000Z/2022-08-03T23:00:00.000Z",
                "downloadUrl": "https://apps.mypurecloud.de/platform/api/v2/downloads/998227281b1efb7b",
                "viewType": "QUEUE_PERFORMANCE_DETAIL_VIEW",
                "period": "PT30M",
                "filter": {
                    "mediaTypes": ["chat"],
                    "queueIds": ["780807e6-83b9-44be-aff0-a41c37fab004"],
                    "directions": ["inbound"],
                },
                "read": False,
                "createdDateTime": "2022-08-03T11:19:39Z",
                "modifiedDateTime": "2022-08-03T11:19:41Z",
                "locale": "en-us",
                "percentageComplete": 1.0,
                "hasFormatDurations": False,
                "hasSplitFilters": True,
                "excludeEmptyRows": True,
                "hasSplitByMedia": True,
                "hasSummaryRow": True,
                "csvDelimiter": "COMMA",
                "hasCustomParticipantAttributes": True,
                "dateLastConfirmed": "2022-08-03T11:19:39Z",
                "intervalKeyType": "ConversationStart",
                "enabled": False,
                "selfUri": "/api/v2/analytics/reporting/exports/30173782-7f79-40d3-9e28-0c80cee6569c",
            },
            {
                "id": "2c0f53f4-c1c8-406b-a48e-0c851e73b894",
                "name": "QUEUE_PERFORMANCE_DETAIL_VIEW_callback",
                "runId": "26b8d2df-49f9-4e95-b3f2-2ea6765f2f31",
                "status": "COMPLETED",
                "timeZone": "UTC",
                "exportFormat": "CSV",
                "interval": "2022-08-02T23:00:00.000Z/2022-08-03T23:00:00.000Z",
                "downloadUrl": "https://apps.mypurecloud.de/platform/api/v2/downloads/ed96ce9f73de06e4",
                "viewType": "QUEUE_PERFORMANCE_DETAIL_VIEW",
                "period": "PT30M",
                "filter": {
                    "mediaTypes": ["callback"],
                    "queueIds": ["383ee5e5-5d8f-406c-ad53-835b69fe82c5"],
                    "directions": ["inbound"],
                },
                "read": False,
                "createdDateTime": "2022-08-03T11:19:31Z",
                "modifiedDateTime": "2022-08-03T11:19:33Z",
                "locale": "en-us",
                "percentageComplete": 1.0,
                "hasFormatDurations": False,
                "hasSplitFilters": True,
                "excludeEmptyRows": True,
                "hasSplitByMedia": True,
                "hasSummaryRow": True,
                "csvDelimiter": "COMMA",
                "hasCustomParticipantAttributes": True,
                "dateLastConfirmed": "2022-08-03T11:19:31Z",
                "intervalKeyType": "ConversationStart",
                "enabled": False,
                "selfUri": "/api/v2/analytics/reporting/exports/2c0f53f4-c1c8-406b-a48e-0c851e73b894",
            },
            {
                "id": "1813992e-557f-466c-a426-bb4c93df140f",
                "name": "QUEUE_PERFORMANCE_DETAIL_VIEW_callback",
                "runId": "964d75b7-e2b3-4ca4-89ee-15022c53b43a",
                "status": "COMPLETED",
                "timeZone": "UTC",
                "exportFormat": "CSV",
                "interval": "2022-08-02T23:00:00.000Z/2022-08-03T23:00:00.000Z",
                "downloadUrl": "https://apps.mypurecloud.de/platform/api/v2/downloads/87a473f45f72bc3e",
                "viewType": "QUEUE_PERFORMANCE_DETAIL_VIEW",
                "period": "PT30M",
                "filter": {
                    "mediaTypes": ["callback"],
                    "queueIds": ["780807e6-83b9-44be-aff0-a41c37fab004"],
                    "directions": ["inbound"],
                },
                "read": False,
                "createdDateTime": "2022-08-03T11:19:31Z",
                "modifiedDateTime": "2022-08-03T11:19:32Z",
                "locale": "en-us",
                "percentageComplete": 1.0,
                "hasFormatDurations": False,
                "hasSplitFilters": True,
                "excludeEmptyRows": True,
                "hasSplitByMedia": True,
                "hasSummaryRow": True,
                "csvDelimiter": "COMMA",
                "hasCustomParticipantAttributes": True,
                "dateLastConfirmed": "2022-08-03T11:19:30Z",
                "intervalKeyType": "ConversationStart",
                "enabled": False,
                "selfUri": "/api/v2/analytics/reporting/exports/1813992e-557f-466c-a426-bb4c93df140f",
            },
            {
                "id": "da92aa23-4120-4edc-8627-7f6d7d5477cd",
                "name": "QUEUE_PERFORMANCE_DETAIL_VIEW_callback",
                "runId": "b09a3299-ed7f-4fc9-8db5-f1048787c226",
                "status": "COMPLETED",
                "timeZone": "UTC",
                "exportFormat": "CSV",
                "interval": "2022-08-02T23:00:00.000Z/2022-08-03T23:00:00.000Z",
                "downloadUrl": "https://apps.mypurecloud.de/platform/api/v2/downloads/0ea70b9e647f5d81",
                "viewType": "QUEUE_PERFORMANCE_DETAIL_VIEW",
                "period": "PT30M",
                "filter": {
                    "mediaTypes": ["callback"],
                    "queueIds": ["383ee5e5-5d8f-406c-ad53-835b69fe82c5"],
                    "directions": ["inbound"],
                },
                "read": False,
                "createdDateTime": "2022-08-03T11:18:42Z",
                "modifiedDateTime": "2022-08-03T11:18:44Z",
                "locale": "en-us",
                "percentageComplete": 1.0,
                "hasFormatDurations": False,
                "hasSplitFilters": True,
                "excludeEmptyRows": True,
                "hasSplitByMedia": True,
                "hasSummaryRow": True,
                "csvDelimiter": "COMMA",
                "hasCustomParticipantAttributes": True,
                "dateLastConfirmed": "2022-08-03T11:18:42Z",
                "intervalKeyType": "ConversationStart",
                "enabled": False,
                "selfUri": "/api/v2/analytics/reporting/exports/da92aa23-4120-4edc-8627-7f6d7d5477cd",
            },
            {
                "id": "7d7542e8-ed96-49d5-a9b7-faad040eef7f",
                "name": "QUEUE_PERFORMANCE_DETAIL_VIEW_callback",
                "runId": "78b88ace-6574-4957-a3a5-12b2c8b1ba85",
                "status": "COMPLETED",
                "timeZone": "UTC",
                "exportFormat": "CSV",
                "interval": "2022-08-02T23:00:00.000Z/2022-08-03T23:00:00.000Z",
                "downloadUrl": "https://apps.mypurecloud.de/platform/api/v2/downloads/d3f059d35b37c2ec",
                "viewType": "QUEUE_PERFORMANCE_DETAIL_VIEW",
                "period": "PT30M",
                "filter": {
                    "mediaTypes": ["callback"],
                    "queueIds": ["780807e6-83b9-44be-aff0-a41c37fab004"],
                    "directions": ["inbound"],
                },
                "read": False,
                "createdDateTime": "2022-08-03T11:18:42Z",
                "modifiedDateTime": "2022-08-03T11:18:43Z",
                "locale": "en-us",
                "percentageComplete": 1.0,
                "hasFormatDurations": False,
                "hasSplitFilters": True,
                "excludeEmptyRows": True,
                "hasSplitByMedia": True,
                "hasSummaryRow": True,
                "csvDelimiter": "COMMA",
                "hasCustomParticipantAttributes": True,
                "dateLastConfirmed": "2022-08-03T11:18:42Z",
                "intervalKeyType": "ConversationStart",
                "enabled": False,
                "selfUri": "/api/v2/analytics/reporting/exports/7d7542e8-ed96-49d5-a9b7-faad040eef7f",
            },
        ],
        "pageSize": 100,
        "pageNumber": 1,
        "total": 6,
        "pageCount": 1,
    }
    g = Genesys()
    g.get_reporting_exports_data()
    mock_load_reports.assert_called_once()


@mock.patch.object(Genesys, "download_report")
@pytest.mark.dependency(
    depends=[
        "test_generate_body",
        "test_generate_exports",
        "test_generate_reports_list",
    ]
)
@pytest.mark.download
def test_download_reports(mock_download_files, var_dictionary):
    g = Genesys()
    g.report_data = var_dictionary["report_data"]
    file_name_list = g.download_all_reporting_exports()

    assert type(file_name_list) == list and len(file_name_list) > 0
    mock_download_files.assert_called()


@mock.patch("viadot.sources.genesys.handle_api_response", return_value=MockClass)
@pytest.mark.dependency(
    depends=[
        "test_generate_body",
        "test_generate_exports",
        "test_generate_reports_list",
        "test_download_reports",
    ]
)
@pytest.mark.delete
def test_genesys_delete_reports(mock_api_response, var_dictionary):
    g = Genesys()
    g.report_data = var_dictionary["report_data"]
    g.delete_all_reporting_exports()
    mock_api_response.assert_called()
