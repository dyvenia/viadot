from datetime import datetime
from unittest import mock

import pytest

from viadot.tasks import GenesysToCSV


@pytest.fixture
def var_dictionary() -> None:
    """Function where variables are stored."""

    variables = {
        "start_date": datetime.now().strftime("%Y-%m-%d"),
        "end_date": datetime.now().strftime("%Y-%m-%d"),
        "v_list": [
            "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxx1",
            "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxx2",
            "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxx3",
        ],
        "key_list": [
            "MainIntent",
            "SubIntent",
            "Final Sub Intent",
            "CustomerOutcomeTrack",
            "LastUtterance",
            "Final Main Intent",
        ],
        "post_data_list": [
            {
                "name": "AGENT_STATUS_DETAIL_VIEW",
                "timeZone": "UTC",
                "exportFormat": "CSV",
                "interval": "2023-03-01T00:00:00/2023-03-02T00:00:00",
                "period": "PT30M",
                "viewType": "AGENT_STATUS_DETAIL_VIEW",
                "filter": {"userIds": ["9eb0fe4e-937e-4443-a5a4-d1b5dbd76520"]},
                "read": True,
                "locale": "en-us",
                "hasFormatDurations": False,
                "hasSplitFilters": True,
                "excludeEmptyRows": True,
                "hasSummaryRow": False,
                "csvDelimiter": "COMMA",
                "hasCustomParticipantAttributes": True,
            }
        ],
        "post_data_list_2": [
            {
                "interval": "2023-03-09T00:00:00/2023-03-10T00:00:00",
                "paging": {"pageSize": 100, "pageNumber": 1},
            }
        ],
    }
    return variables


class MockGenesysTask:
    report_data = ([[None, "COMPLETED"], [None, "COMPLETED"]],)

    def genesys_api_connection(post_data_list, end_point, method="POST"):
        if method == "GET":
            report = {
                "id": "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx",
                "startTime": "2023-06-28T10:59:48.194Z",
                "participants": [
                    {
                        "id": "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx",
                        "startTime": "2023-06-28T10:59:48.194Z",
                        "connectedTime": "2023-06-28T10:59:48.194Z",
                        "externalContactId": "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx",
                        "queueId": "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx",
                        "queueName": "dev_name",
                        "purpose": "customer",
                        "wrapupRequired": None,
                        "mediaRoles": ["full"],
                        "attributes": {
                            "MainIntent3": "mainintent3",
                            "SubIntent3": "subintent3",
                            "SubIntent2": "subintent2",
                            "MainIntent4": "mainintent4",
                            "MainIntent1": "mainintent1",
                            "SubIntent1": "subintent1",
                            "MainIntent2": "mainintent2",
                            "Final Sub Intent": "finalsubintent",
                            "SubIntent4": "subintent4",
                            "CustomerOutcomeTrack4": "customeroutcome4",
                            "CustomerOutcomeTrack3": "customeroutcome3",
                            "CustomerOutcomeTrack2": "customeroutcome2",
                            "CustomerOutcomeTrack1": "customeroutcome1",
                            "LastUtterance2": "lastutterance2",
                            "reached": "reach",
                            "LastUtterance1": "lastutterance1",
                            "name": "dev_name",
                            "Final Main Intent": "finalmainintent",
                            "LastUtterance4": "lastutterance4",
                            "LastUtterance3": "lastutterance3",
                            "LOB": "lob",
                            "memberId": "123456789",
                        },
                        "calls": [],
                        "callbacks": [],
                        "chats": [],
                        "cobrowsesessions": [],
                        "emails": [],
                        "messages": [],
                    }
                ],
            }
        else:
            report = {
                "conversations": [
                    {
                        "conversationEnd": "2020-01-01T00:00:00.00Z",
                        "conversationId": "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx",
                        "conversationStart": "2020-01-01T00:00:00.00Z",
                        "divisionIds": [
                            "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx",
                            "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx",
                        ],
                        "mediaStatsMinConversationMos": 4.379712366260067,
                        "mediaStatsMinConversationRFactor": 79.03050231933594,
                        "originatingDirection": "inbound",
                        "participants": [
                            {
                                "externalContactId": "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx",
                                "participantId": "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx",
                                "participantName": "Mobile Number, Country",
                                "purpose": "customer",
                                "sessions": [
                                    {
                                        "agentBullseyeRing": 1,
                                        "ani": "tel:+xxxxxxxxxxx",
                                        "direction": "inbound",
                                        "dnis": "tel:+xxxxxxxxxxx",
                                        "edgeId": "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx",
                                        "mediaType": "voice",
                                        "protocolCallId": "xxxxxxxxxxxxxxxxxxx@xx.xxx.xxx.xxx",
                                        "provider": "Edge",
                                        "remoteNameDisplayable": "Mobile Number, Country",
                                        "requestedRoutings": ["Standard"],
                                        "routingRing": 1,
                                        "selectedAgentId": "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx",
                                        "sessionDnis": "tel:+xxxxxxxxxxx",
                                        "sessionId": "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx",
                                        "usedRouting": "Standard",
                                        "mediaEndpointStats": [
                                            {
                                                "codecs": ["audio/opus"],
                                                "eventTime": "2020-01-01T00:00:00.00Z",
                                                "maxLatencyMs": 30,
                                                "minMos": 4.882504366160681,
                                                "minRFactor": 92.44775390625,
                                                "receivedPackets": 229,
                                            },
                                        ],
                                        "metrics": [
                                            {
                                                "emitDate": "2020-01-01T00:00:00.00Z",
                                                "name": "nConnected",
                                                "value": 1,
                                            },
                                        ],
                                        "segments": [
                                            {
                                                "conference": False,
                                                "segmentEnd": "2020-01-01T00:00:00.00Z",
                                                "segmentStart": "2020-01-01T00:00:00.00Z",
                                                "segmentType": "system",
                                            },
                                            {
                                                "conference": False,
                                                "disconnectType": "peer",
                                                "queueId": "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx",
                                                "segmentEnd": "2020-01-01T00:00:00.00Z",
                                                "segmentStart": "2020-01-01T00:00:00.00Z",
                                                "segmentType": "interact",
                                            },
                                        ],
                                    }
                                ],
                            },
                            {
                                "participantId": "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx",
                                "participantName": "xxxxxxxxxxxxxxxxxxxxx",
                                "purpose": "ivr",
                                "sessions": [
                                    {
                                        "ani": "tel:+xxxxxxxxxxx",
                                        "direction": "inbound",
                                        "dnis": "tel:+xxxxxxxxxxx",
                                        "edgeId": "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx",
                                        "mediaType": "voice",
                                        "peerId": "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx",
                                        "protocolCallId": "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx",
                                        "provider": "Edge",
                                        "remote": "Mobile Number, Country",
                                        "remoteNameDisplayable": "xxxxxxxx, Country",
                                        "sessionDnis": "tel:+xxxxxxxxxxx",
                                        "sessionId": "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx",
                                        "mediaEndpointStats": [
                                            {
                                                "codecs": ["audio/opus"],
                                                "eventTime": "2020-01-01T00:00:00.00Z",
                                                "maxLatencyMs": 30,
                                                "minMos": 4.429814389713434,
                                                "minRFactor": 79.03050231933594,
                                                "receivedPackets": 229,
                                            }
                                        ],
                                        "flow": {
                                            "endingLanguage": "lt-lt",
                                            "entryReason": "tel:+xxxxxxxxxxx",
                                            "entryType": "dnis",
                                            "exitReason": "TRANSFER",
                                            "flowId": "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx",
                                            "flowName": "xxxxxxxxxxxxxxxxxxxxx",
                                            "flowType": "INBOUNDCALL",
                                            "flowVersion": "22.0",
                                            "startingLanguage": "en-us",
                                            "transferTargetAddress": "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx",
                                            "transferTargetName": "xxxxxxxxxxxxxxxxxxxxx",
                                            "transferType": "ACD",
                                        },
                                        "metrics": [
                                            {
                                                "emitDate": "2020-01-01T00:00:00.00Z",
                                                "name": "nFlow",
                                                "value": 1,
                                            },
                                        ],
                                        "segments": [
                                            {
                                                "conference": False,
                                                "segmentEnd": "2020-01-01T00:00:00.00Z",
                                                "segmentStart": "2020-01-01T00:00:00.00Z",
                                                "segmentType": "system",
                                            },
                                        ],
                                    }
                                ],
                            },
                        ],
                    }
                ],
                "totalHits": 100,
            }
        return report

    def get_reporting_exports_data():
        pass

    def delete_all_reporting_exports():
        pass

    def download_all_reporting_exports(path):
        return [
            "V_D_PROD_FB_QUEUE_CALLBACK.csv",
            "V_D_PROD_FB_QUEUE_CHAT.csv",
        ]

    def delete_all_reporting_exports():
        pass


@mock.patch.object(GenesysToCSV, "run")
@pytest.mark.to_csv
def test_genesys_to_csv(mock_reuturn):
    mock_reuturn.return_value = [
        "V_D_PROD_FB_QUEUE_CALLBACK.csv",
        "V_D_PROD_FB_QUEUE_CHAT.csv",
    ]
    genesys_to_csv = GenesysToCSV()
    files_name_list = genesys_to_csv.run()
    assert isinstance(files_name_list, list)


@mock.patch("viadot.tasks.genesys.Genesys", return_value=MockGenesysTask)
@pytest.mark.files
def test_genesys_files_type(mock_genesys, var_dictionary):
    to_csv = GenesysToCSV()
    file_name = to_csv.run(
        view_type="agent_status_detail_view",
        view_type_time_sleep=5,
        post_data_list=var_dictionary["post_data_list"],
        start_date=var_dictionary["start_date"],
    )
    mock_genesys.assert_called_once()
    assert len(file_name) > 1


@mock.patch("viadot.tasks.genesys.Genesys", return_value=MockGenesysTask)
@pytest.mark.conv
def test_genesys_conversations(mock_genesys, var_dictionary):
    to_csv = GenesysToCSV()
    file_name = to_csv.run(
        view_type=None,
        end_point="analytics/conversations/details/query",
        post_data_list=var_dictionary["post_data_list_2"],
        start_date=var_dictionary["start_date"],
    )
    date = var_dictionary["start_date"].replace("-", "")

    mock_genesys.assert_called_once()
    assert file_name[0] == f"conversations_detail_{date}".upper() + ".csv"


@mock.patch("viadot.tasks.genesys.Genesys", return_value=MockGenesysTask)
@pytest.mark.conv
def test_genesys_webmsg(mock_genesys, var_dictionary):
    to_csv = GenesysToCSV()
    file_name = to_csv.run(
        view_type=None,
        end_point="conversations",
        conversationId_list=var_dictionary["v_list"],
        post_data_list=[""],
        key_list=var_dictionary["key_list"],
        start_date=var_dictionary["start_date"],
        end_date=var_dictionary["end_date"],
    )

    start = var_dictionary["start_date"].replace("-", "")
    end = var_dictionary["end_date"].replace("-", "")

    mock_genesys.assert_called_once()
    assert file_name[0] == f"WEBMESSAGE_{start}-{end}.csv"
