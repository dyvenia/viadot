import json
import logging
from unittest.mock import AsyncMock, MagicMock, patch
import warnings

import pandas as pd
import pytest

from viadot.exceptions import APIError
from viadot.sources import Genesys


warnings.filterwarnings("ignore", category=DeprecationWarning)

variables = {
    "credentials": {
        "client_id": "test_client_id",
        "client_secret": "test_client_secret",
    },
    "request_headers": {
        "token_type": "Bearer",
        "access_token": "test_access_token",
    },
    "headers": {
        "Authorization": "Bearer access_token",
        "Content-Type": "application/json",
    },
    "entities": [
        {
            "id": "report1",
            "downloadUrl": "http://example.com/report1",
            "status": "COMPLETED",
        },
        {
            "id": "report2",
            "downloadUrl": "http://example.com/report2",
            "status": "COMPLETED",
        },
    ],
    "entities_fail": [
        {
            "id": "report1",
            "downloadUrl": "http://example.com/report1",
            "status": "FAILED",
        },
    ],
    "entities_run": [
        {
            "id": "report1",
            "downloadUrl": "http://example.com/report1",
            "status": "RUNNING",
        },
    ],
    "content": b"id,name\n1,Report1\n2,Report2",
    "expected_data": {"id": [1, 2], "name": ["Report1", "Report2"]},
    "report_url": "http://example.com/report",
    "data_to_merge": [
        {
            "conversationId": "conv1",
            "participants": [
                {
                    "externalContactId": "ext1",
                    "participantId": "part1",
                    "sessions": [
                        {
                            "sessionId": "sess1",
                        }
                    ],
                }
            ],
        }
    ],
    "expected_columns": [
        "conversationId",
        "externalContactId",
        "participantId",
        "sessionId",
    ],
    "expected_data_to_merge": {
        "conversationId": ["conv1"],
        "externalContactId": ["ext1"],
        "participantId": ["part1"],
        "sessionId": ["sess1"],
    },
    "mock_post_data_list": [{"data": "some data"}, {"data2": "some data2"}],
    "mock_report": {
        "conversations": [
            {"conversationId": "conv1", "participants": [{"sessionId": "sess1"}]}
        ],
        "totalHits": 100,
    },
}


@pytest.fixture
def genesys():
    """Return Genesys instance."""
    return Genesys(credentials=variables["credentials"], verbose=True)


@patch("viadot.sources.genesys.get_source_credentials", return_value=None)
def test_init_no_credentials(mock_get_source_credentials):
    """Test raise error without credentials."""
    with pytest.raises(TypeError):
        Genesys()

    mock_get_source_credentials.assert_called_once()


def test_init_invalid_environment():
    """Test Genesys invalid environment."""
    with pytest.raises(APIError):
        Genesys(
            credentials=variables["credentials"],
            environment="invalid_environment",
        )


@patch("viadot.sources.genesys.handle_api_response")
def test_headers(mock_handle_api_response, genesys):
    """Test Genesys `headers` property."""
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = variables["request_headers"]
    mock_handle_api_response.return_value = mock_response

    headers = genesys.headers
    assert headers["Authorization"] == "Bearer test_access_token"


@pytest.mark.skip(reason="Needs to be fixed.")
@patch("aiohttp.ClientSession.post", new_callable=AsyncMock)
@pytest.mark.asyncio
async def test_api_call_post_success(mock_post, genesys):
    """Test Genesys `_api_call()` method called with POST."""
    mock_response = AsyncMock()
    mock_response.read.return_value = json.dumps({"key": "value"}).encode("utf-8")
    mock_post.return_value.__aenter__.return_value = mock_response

    response = genesys._api_call(
        endpoint="test_endpoint",
        post_data_list=[{"data_key": "data_value"}],
        method="POST",
    )

    assert response == {"key": "value"}
    mock_post.assert_called_once_with(
        f"https://api.{genesys.environment}/api/v2/test_endpoint",
        headers=genesys.headers,
        data=json.dumps({"data_key": "data_value"}),
    )


@pytest.mark.skip(reason="Needs to be fixed.")
@patch("aiohttp.ClientSession.post", new_callable=AsyncMock)
@pytest.mark.asyncio
def test_api_call_get_success(mock_get, genesys):
    """Test Genesys `_api_call()` method called with GET."""
    mock_response = AsyncMock()
    mock_response.read.return_value = json.dumps({"key": "value"}).encode("utf-8")
    mock_get.return_value.__aenter__.return_value = mock_response

    response = genesys._api_call(
        endpoint="test_endpoint",
        post_data_list=[],
        method="GET",
        params={"param1": "value1"},
    )

    assert response == {"key": "value"}
    mock_get.assert_called_once_with(
        f"https://api.{genesys.environment}/api/v2/test_endpoint",
        headers=genesys.headers,
        params={"param1": "value1"},
    )


@pytest.mark.skip(reason="Needs to be fixed.")
@patch("aiohttp.ClientSession.post", new_callable=AsyncMock)
@pytest.mark.asyncio
async def test_api_call_post_failure(mock_post, genesys):
    """Test Genesys `_api_call` method failing when called with POST."""
    mock_response = AsyncMock()
    mock_response.read.return_value = b"Bad Request"
    mock_response.status = 400
    mock_post.return_value.__aenter__.return_value = mock_response

    with pytest.raises(APIError) as context:
        genesys._api_call(
            endpoint="test_endpoint",
            post_data_list=[{"data_key": "data_value"}],
            method="POST",
        )

    assert "API call failed" in str(context.exception)
    mock_post.assert_called_once_with(
        f"https://api.{genesys.environment}/api/v2/test_endpoint",
        headers=genesys.headers,
        data=json.dumps({"data_key": "data_value"}),
    )


@patch("viadot.sources.genesys.handle_api_response")
@patch.object(Genesys, "headers", new_callable=MagicMock)
def test_load_reporting_exports(mock_auth_token, mock_handle_api_response, genesys):
    """Test Genesys `_load_reporting_exports` method."""
    mock_auth_token.return_value = variables["headers"]

    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = {"entities": []}
    mock_handle_api_response.return_value = mock_response

    result = genesys._load_reporting_exports()
    assert result == {"entities": []}


@patch("viadot.sources.genesys.handle_api_response")
@patch.object(Genesys, "headers", new_callable=MagicMock)
def test_load_reporting_exports_failure(
    mock_auth_token, mock_handle_api_response, genesys
):
    """Test Genesys `_load_reporting_exports` method failing."""
    mock_auth_token.return_value = variables["headers"]

    mock_response = MagicMock()
    mock_response.status_code = 400
    mock_handle_api_response.return_value = mock_response

    with pytest.raises(APIError) as e:
        genesys._load_reporting_exports()

    assert "Failed to loaded all exports." in str(e)


def test_get_reporting_exports_url(genesys, caplog):
    """Test Genesys `_get_reporting_exports_url` method."""
    entities = variables["entities"]

    with caplog.at_level(logging.INFO):
        actual_ids, actual_urls = genesys._get_reporting_exports_url(entities)
        msg = "Report status:\n\treport1 -> COMPLETED \n\treport2 -> COMPLETED \n"
        assert msg in caplog.text

    expected_ids = ["report1", "report2"]
    expected_urls = ["http://example.com/report1", "http://example.com/report2"]

    assert expected_ids == actual_ids
    assert expected_urls == actual_urls


def test_get_reporting_exports_url_with_failed_status(genesys, caplog):
    """Test Genesys `_get_reporting_exports_url` method FAILED status."""
    entities = variables["entities_fail"]

    with caplog.at_level(logging.ERROR):
        actual_ids, actual_urls = genesys._get_reporting_exports_url(entities)
        msg = "Some reports have not been successfully created."
        assert msg in caplog.text

    expected_ids = ["report1"]
    expected_urls = ["http://example.com/report1"]

    assert expected_ids == actual_ids
    assert expected_urls == actual_urls


def test_get_reporting_exports_url_with_running_status(genesys, caplog):
    """Test Genesys `_get_reporting_exports_url` method RUNNING status."""
    entities = variables["entities_run"]

    with caplog.at_level(logging.WARNING):
        actual_ids, actual_urls = genesys._get_reporting_exports_url(entities)
        msg = "Some reports are still being created and can not be downloaded."
        assert msg in caplog.text

    expected_ids = ["report1"]
    expected_urls = ["http://example.com/report1"]
    assert expected_ids == actual_ids
    assert expected_urls == actual_urls


@patch("viadot.sources.genesys.handle_api_response")
def test_download_report_success(mock_handle_api_response, genesys, caplog):
    """Test Genesys `_download_report` method."""
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.content = variables["content"]
    mock_handle_api_response.return_value = mock_response

    expected_data = variables["expected_data"]
    expected_df = pd.DataFrame(expected_data)

    report_url = variables["report_url"]

    with caplog.at_level(logging.INFO):
        actual_df = genesys._download_report(report_url)
        msg = "Successfully downloaded report from Genesys API ('http://example.com/report')."
        assert msg in caplog.text

    assert expected_df.equals(actual_df)


@patch("viadot.sources.genesys.handle_api_response")
def test_download_report_failure(mock_handle_api_response, genesys, caplog):
    """Test Genesys `_download_report` method failure."""
    mock_response = MagicMock()
    mock_response.status_code = 404
    mock_response.content = b"Not Found"
    mock_handle_api_response.return_value = mock_response

    report_url = variables["report_url"]

    with caplog.at_level(logging.ERROR):
        genesys._download_report(report_url)
        msg = "Failed to download report from Genesys API ('http://example.com/report'). - b'Not Found'"
        assert msg in caplog.text


@patch("viadot.sources.genesys.handle_api_response")
def test_download_report_drop_duplicates(mock_handle_api_response, genesys):
    """Test Genesys `_download_report` method, dropping duplicates."""
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.content = variables["content"]
    mock_handle_api_response.return_value = mock_response

    expected_data = variables["expected_data"]
    expected_df = pd.DataFrame(expected_data)

    report_url = variables["report_url"]
    actual_df = genesys._download_report(
        report_url,
        drop_duplicates=True,
    )

    assert expected_df.equals(actual_df)


@patch("viadot.sources.genesys.handle_api_response")
def test_delete_report_success(mock_handle_api_response, genesys, caplog):
    """Test Genesys `_delete_report` method."""
    mock_response = MagicMock()
    mock_response.status_code = 204
    mock_handle_api_response.return_value = mock_response

    report_id = "123456"

    with caplog.at_level(logging.INFO):
        genesys._delete_report(report_id)

    msg = f"Successfully deleted report '{report_id}' from Genesys API."
    assert msg in caplog.text


@patch("viadot.sources.genesys.handle_api_response")
def test_delete_report_failure(mock_handle_api_response, caplog, genesys):
    """Test Genesys `_delete_report` method failure."""
    mock_response = MagicMock()
    mock_response.status_code = 404
    mock_response.content = b"Not Found"
    mock_handle_api_response.return_value = mock_response

    report_id = "123456"

    with caplog.at_level(logging.ERROR):
        genesys._delete_report(report_id)

    msg = f"Failed to delete report '{report_id}' from Genesys API. - b'Not Found'"
    assert msg in caplog.text


def test_merge_conversations(genesys):
    """Test Genesys `_merge_conversations` function."""
    mock_data = variables["data_to_merge"]
    expected_columns = variables["expected_columns"]
    expected_data = variables["expected_data_to_merge"]

    result_df = genesys._merge_conversations(mock_data)

    assert list(result_df.columns) == expected_columns
    for col, expected_values in expected_data.items():
        assert list(result_df[col]) == expected_values


@patch("viadot.sources.genesys.Genesys._api_call")
@patch("viadot.sources.genesys.Genesys._load_reporting_exports")
@patch("viadot.sources.genesys.Genesys._get_reporting_exports_url")
@patch("viadot.sources.genesys.Genesys._download_report")
@patch("viadot.sources.genesys.Genesys._delete_report")
def test_api_connection_reporting_exports(
    mock_delete, mock_download, mock_get_url, mock_load, mock_api_call, genesys
):
    """Test Genesys `api_connection` method with reporting exports."""
    mock_entities = variables["entities"]
    mock_load.return_value = {"entities": mock_entities}
    mock_get_url.return_value = (
        ["report1", "report2"],
        ["http://example.com/report1", "http://example.com/report2"],
    )
    mock_download.return_value = pd.DataFrame(
        {"Queue Id": ["queue1;queue2"], "data": [1]}
    )

    genesys.api_connection(
        endpoint="analytics/reporting/exports",
        post_data_list=variables["mock_post_data_list"],
        view_type="queue_performance_detail_view",
        view_type_time_sleep=0.5,
    )

    mock_api_call.assert_called_once_with(
        endpoint="analytics/reporting/exports",
        post_data_list=variables["mock_post_data_list"],
        method="POST",
        time_between_api_call=0.5,
    )
    mock_load.assert_called_once()
    mock_get_url.assert_called_once()
    mock_download.assert_called()
    mock_delete.assert_called()


@patch("viadot.sources.genesys.Genesys._api_call")
@patch("viadot.sources.genesys.Genesys._merge_conversations")
def test_api_connection_conversations(mock_merge, mock_api_call, genesys):
    """Test Genesys `api_connection` method with conversations details."""
    mock_post_data_list = [{"paging": {"pageNumber": 1}}]
    mock_report = variables["mock_report"]
    mock_merge.return_value = pd.DataFrame({"conversationId": ["conv1"], "data": [1]})
    mock_api_call.side_effect = [mock_report, mock_report]

    genesys.api_connection(
        endpoint="analytics/conversations/details/query",
        post_data_list=mock_post_data_list,
    )

    mock_api_call.assert_called()
    mock_merge.assert_called()


@patch("viadot.sources.genesys.Genesys._api_call")
def test_api_connection_routing_queues_members(mock_api_call, genesys):
    """Test Genesys `api_connection` method with routing queues."""
    mock_queues_ids = ["queue1"]
    mock_response_page_1 = {"entities": [{"userId": "user1", "name": "Agent1"}]}
    mock_response_page_2 = {"entities": []}
    mock_api_call.side_effect = [mock_response_page_1, mock_response_page_2]

    genesys.api_connection(
        endpoint="routing_queues_members", queues_ids=mock_queues_ids
    )

    assert mock_api_call.call_count == 2


@patch("viadot.sources.genesys.Genesys._api_call")
def test_api_connection_users(mock_api_call, genesys):
    """Test Genesys `api_connection` method with users."""
    mock_queues_ids = ["queue1"]
    mock_response_page_1 = {"entities": [{"userId": "user1", "name": "Agent1"}]}
    mock_response_page_2 = {"entities": []}
    mock_api_call.side_effect = [mock_response_page_1, mock_response_page_2]

    genesys.api_connection(endpoint="users", queues_ids=mock_queues_ids)

    assert mock_api_call.call_count == 2


@patch("viadot.sources.genesys.Genesys._handle_if_empty")
@patch("viadot.sources.genesys.super")
def test_to_df(mock_super, mock_handle_if_empty, genesys):
    """Test Genesys `to_df` method."""
    mock_super().to_df = MagicMock()
    mock_handle_if_empty = MagicMock()
    genesys.data_returned = {
        0: pd.DataFrame({"A": [1, 2], "B": [3, 4]}),
        1: pd.DataFrame({"A": [2, 3], "B": [4, 5]}),
    }

    result_df = genesys.to_df()
    result_df.drop(
        columns=["_viadot_source", "_viadot_downloaded_at_utc"],
        inplace=True,
        axis=1,
    )
    expected_df = pd.DataFrame({"A": [1, 2, 2, 3], "B": [3, 4, 4, 5]})

    assert result_df.equals(expected_df)
    mock_super().to_df.assert_called_once()
    mock_handle_if_empty.assert_not_called()


@patch("viadot.sources.genesys.Genesys._handle_if_empty")
@patch("viadot.sources.genesys.super")
def test_to_df_duplicates(mock_super, mock_handle_if_empty, genesys):
    """Test Genesys `to_df` method, dropping duplicates."""
    mock_super().to_df = MagicMock()
    mock_handle_if_empty = MagicMock()
    genesys.data_returned = {
        0: pd.DataFrame({"A": [1, 2], "B": [3, 4]}),
        1: pd.DataFrame({"A": [2, 3], "B": [4, 5]}),
    }

    expected_df_no_duplicates = pd.DataFrame({"A": [1, 2, 3], "B": [3, 4, 5]})
    result_df_no_duplicates = genesys.to_df(drop_duplicates=True)
    result_df_no_duplicates.drop(
        columns=["_viadot_source", "_viadot_downloaded_at_utc"],
        inplace=True,
        axis=1,
    )

    assert result_df_no_duplicates.equals(expected_df_no_duplicates)
    mock_super().to_df.assert_called_once()
    mock_handle_if_empty.assert_not_called()


@patch("viadot.sources.genesys.validate")
@patch("viadot.sources.genesys.Genesys._handle_if_empty")
@patch("viadot.sources.genesys.super")
def test_to_df_validate(mock_super, mock_handle_if_empty, mock_validate, genesys):
    """Test Genesys `to_df` method, checking validation function."""
    mock_super().to_df = MagicMock()
    mock_handle_if_empty = MagicMock()
    genesys.data_returned = {
        0: pd.DataFrame({"A": [1, 2], "B": [3, 4]}),
        1: pd.DataFrame({"A": [2, 3], "B": [4, 5]}),
    }

    validate_df_dict = {"some_key": "some_value"}
    result_df = genesys.to_df(validate_df_dict=validate_df_dict)
    result_df.drop(
        columns=["_viadot_source", "_viadot_downloaded_at_utc"],
        inplace=True,
        axis=1,
    )

    mock_validate.assert_called_once_with(df=result_df, tests=validate_df_dict)
    mock_super().to_df.assert_called_once()
    mock_handle_if_empty.assert_not_called()


@patch("viadot.sources.genesys.Genesys._handle_if_empty")
@patch("viadot.sources.genesys.super")
def test_to_df_empty(mock_super, mock_handle_if_empty, genesys):
    """Test Genesys `to_df` method, checking empty response."""
    mock_super().to_df = MagicMock()
    mock_handle_if_empty = MagicMock()
    genesys.data_returned = {
        0: pd.DataFrame({"A": [1, 2], "B": [3, 4]}),
        1: pd.DataFrame({"A": [2, 3], "B": [4, 5]}),
    }

    genesys.data_returned = {}
    result_df_empty = genesys.to_df()

    assert result_df_empty.empty
    mock_super().to_df.assert_called_once()
    mock_handle_if_empty.assert_not_called()
