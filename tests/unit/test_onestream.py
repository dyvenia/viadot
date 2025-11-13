"""Tests for the OneStream class."""

import json
from unittest.mock import MagicMock, patch

import pandas as pd
from pydantic import ValidationError
import pytest
import requests

from viadot.sources import OneStream
from viadot.sources.onestream import OneStreamCredentials


DUMMY_CREDS = {
    "api_token": "test_api_token_123456",  # pragma: allowlist secret
}

SAMPLE_ADAPTER_RESPONSE = {
    "Results": [
        {"ID": 1, "Entity": "Entity1", "Account": "Account1", "Amount": 1000.50},
        {"ID": 2, "Entity": "Entity2", "Account": "Account2", "Amount": 2000.75},
        {"ID": 3, "Entity": "Entity1", "Account": "Account3", "Amount": 1500.25},
    ]
}

SAMPLE_SQL_RESPONSE = {
    "Results": [
        {"UserName": "admin", "FullName": "Administrator", "IsActive": True},
        {"UserName": "user1", "FullName": "User One", "IsActive": True},
        {"UserName": "user2", "FullName": "User Two", "IsActive": False},
    ]
}

SAMPLE_CUSTOM_SUBST_VARS = {
    "prm_entity": ["Entity1", "Entity2"],
    "prm_scenario": ["Actual", "Budget"],
}


@pytest.fixture
def onestream_credentials():
    """Sample OneStream credentials for testing."""
    return DUMMY_CREDS


# TODO: Check if this is enough
@pytest.fixture
def onestream_instance(onestream_credentials):
    """Create OneStream instance with test credentials."""
    return OneStream(
        server_url="https://test.onestream.com",
        application="TestApp",
        credentials=onestream_credentials,
    )


def test_valid_credentials():
    """Test creating valid OneStreamCredentials."""
    creds = OneStreamCredentials(api_token="test_token")  # noqa: S106
    assert creds.api_token == "test_token"  # noqa: S105


def test_missing_api_token():
    """Test OneStreamCredentials with missing api_token."""
    with pytest.raises(ValueError, match="api_token"):
        OneStreamCredentials()


def test_credentials_from_dict():
    """Test creating credentials from dictionary."""
    creds_dict = {"api_token": "dict_token"}
    creds = OneStreamCredentials(**creds_dict)
    assert creds.api_token == "dict_token"  # noqa: S105


@patch("viadot.sources.onestream.get_source_credentials")
def test_init_with_credentials_dict(mock_get_creds, onestream_credentials):
    """Test initialization with credentials dictionary."""
    mock_get_creds.return_value = None
    onestream = OneStream(
        server_url="https://test.onestream.com",
        application="TestApp",
        credentials=onestream_credentials,
    )
    assert onestream.credentials == onestream_credentials
    assert onestream.server_url == "https://test.onestream.com"
    assert onestream.application == "TestApp"
    assert onestream.api_token == "test_api_token_123456"  # noqa: S105


@patch("viadot.sources.onestream.get_source_credentials")
def test_init_with_config_key(mock_get_creds, onestream_credentials):
    """Test initialization with config key."""
    mock_get_creds.return_value = onestream_credentials
    onestream = OneStream(
        server_url="https://test.onestream.com",
        application="TestApp",
        config_key="test_onestream",
    )
    assert onestream.credentials == onestream_credentials


@patch("viadot.sources.onestream.get_source_credentials")
def test_init_without_credentials_raises_error(mock_get_creds):
    """Test initialization without credentials raises error."""
    mock_get_creds.return_value = None
    with pytest.raises(ValidationError):
        OneStream(
            server_url="https://test.onestream.com",
            application="TestApp",
        )


def test_init_with_custom_api_params():
    """Test initialization with custom api_params."""
    custom_params = {"api-version": "7.2.0", "custom": "value"}
    onestream = OneStream(
        server_url="https://test.onestream.com",
        application="TestApp",
        credentials=DUMMY_CREDS,
        api_params=custom_params,
    )
    assert onestream.api_params == custom_params


def test_unpack_custom_subst_vars_to_string(onestream_instance):
    """Test conversion of custom substitution variables to string format."""
    custom_subst_vars = {"prm_entity": "Entity1", "prm_scenario": "Actual"}
    result = onestream_instance._unpack_custom_subst_vars_to_string(custom_subst_vars)
    expected = "prm_entity=Entity1,prm_scenario=Actual"
    assert result == expected


def test_unpack_custom_subst_vars_to_string_empty(onestream_instance):
    """Test conversion with empty custom vars."""
    result = onestream_instance._unpack_custom_subst_vars_to_string({})
    assert result == ""


def test_get_all_custom_subst_vars_combinations(onestream_instance):
    """Test generation of all combinations of custom variables."""
    custom_subst_vars = {"prm_entity": ["E1", "E2"], "prm_scenario": ["S1", "S2"]}
    result = onestream_instance._get_all_custom_subst_vars_combinations(
        custom_subst_vars
    )

    expected = [
        {"prm_entity": "E1", "prm_scenario": "S1"},
        {"prm_entity": "E1", "prm_scenario": "S2"},
        {"prm_entity": "E2", "prm_scenario": "S1"},
        {"prm_entity": "E2", "prm_scenario": "S2"},
    ]
    assert result == expected
    assert len(result) == 4


def test_get_all_custom_subst_vars_combinations_single_var(onestream_instance):
    """Test combinations with single variable."""
    custom_subst_vars = {"prm_entity": ["E1", "E2", "E3"]}
    result = onestream_instance._get_all_custom_subst_vars_combinations(
        custom_subst_vars
    )

    expected = [
        {"prm_entity": "E1"},
        {"prm_entity": "E2"},
        {"prm_entity": "E3"},
    ]
    assert result == expected
    assert len(result) == 3


def test_get_all_custom_subst_vars_combinations_empty(onestream_instance):
    """Test combinations with empty input."""
    result = onestream_instance._get_all_custom_subst_vars_combinations({})
    assert result == [{}]


@patch("viadot.sources.onestream.requests.post")
def test_send_api_request_success(mock_post, onestream_instance):
    """Test successful API request."""
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.raise_for_status.return_value = None
    mock_post.return_value = mock_response

    endpoint = "https://test.onestream.com/api/test"
    headers = {"Content-Type": "application/json", "Authorization": "Bearer token"}
    payload = '{"test": "data"}'

    result = onestream_instance._send_api_request(endpoint, headers, payload)

    assert result == mock_response
    mock_post.assert_called_once_with(
        url=endpoint,
        params={"api-version": "5.2.0"},
        headers=headers,
        data=payload,
        timeout=(60, 3600),
    )


@patch("viadot.sources.onestream.requests.post")
def test_send_api_request_timeout(mock_post, onestream_instance):
    """Test API request timeout handling."""
    mock_post.side_effect = requests.exceptions.Timeout("Request timed out")

    endpoint = "https://test.onestream.com/api/test"
    headers = {"Content-Type": "application/json"}
    payload = '{"test": "data"}'

    with pytest.raises(requests.exceptions.Timeout):
        onestream_instance._send_api_request(endpoint, headers, payload)


@patch("viadot.sources.onestream.requests.post")
def test_send_api_request_connection_error(mock_post, onestream_instance):
    """Test API request connection error handling."""
    mock_post.side_effect = requests.exceptions.ConnectionError("Connection failed")

    endpoint = "https://test.onestream.com/api/test"
    headers = {"Content-Type": "application/json"}
    payload = '{"test": "data"}'

    with pytest.raises(requests.exceptions.ConnectionError):
        onestream_instance._send_api_request(endpoint, headers, payload)


@patch("viadot.sources.onestream.requests.post")
def test_send_api_request_http_error(mock_post, onestream_instance):
    """Test API request HTTP error handling."""
    mock_response = MagicMock()
    mock_response.raise_for_status.side_effect = requests.exceptions.HTTPError(
        "404 Not Found"
    )
    mock_post.return_value = mock_response

    endpoint = "https://test.onestream.com/api/test"
    headers = {"Content-Type": "application/json"}
    payload = '{"test": "data"}'

    with pytest.raises(requests.exceptions.RequestException):
        onestream_instance._send_api_request(endpoint, headers, payload)


def test_fetch_req_results_success(onestream_instance):
    """Test successful extraction of results from API response."""
    mock_response = MagicMock()
    mock_response.json.return_value = SAMPLE_ADAPTER_RESPONSE

    result = onestream_instance._fetch_req_results(mock_response, "Results")
    assert result == SAMPLE_ADAPTER_RESPONSE["Results"]


def test_fetch_req_results_null_response(onestream_instance):
    """Test handling of null API response."""
    mock_response = MagicMock()
    mock_response.json.return_value = None

    with pytest.raises(ValueError, match="API response is null"):
        onestream_instance._fetch_req_results(mock_response, "Results")


def test_fetch_req_results_missing_key(onestream_instance):
    """Test handling of missing key in API response."""
    mock_response = MagicMock()
    mock_response.json.return_value = {"WrongKey": []}

    with pytest.raises(KeyError):
        onestream_instance._fetch_req_results(mock_response, "Results")


@patch.object(OneStream, "_send_api_request")
@patch.object(OneStream, "_fetch_req_results")
def test_get_adapter_results_data_success(
    mock_fetch_results, mock_send_request, onestream_instance
):
    """Test successful Data Adapter results retrieval."""
    mock_response = MagicMock()
    mock_send_request.return_value = mock_response
    mock_fetch_results.return_value = SAMPLE_ADAPTER_RESPONSE["Results"]

    result = onestream_instance._get_adapter_results_data(
        adapter_name="TestAdapter",
        workspace_name="TestWorkspace",
        adapter_response_key="Results",
        custom_subst_vars={"prm_entity": "Entity1"},
    )

    assert result == SAMPLE_ADAPTER_RESPONSE["Results"]

    # Verify API call was made with correct parameters
    mock_send_request.assert_called_once()
    call_args = mock_send_request.call_args
    endpoint = call_args[0][0]
    headers = call_args[0][1]
    payload_str = call_args[0][2]
    payload = json.loads(payload_str)

    assert "/api/DataProvider/GetAdoDataSetForAdapter" in endpoint
    assert headers["Content-Type"] == "application/json"
    assert headers["Authorization"] == "Bearer test_api_token_123456"
    assert payload["AdapterName"] == "TestAdapter"
    assert payload["WorkspaceName"] == "TestWorkspace"
    assert payload["CustomSubstVarsAsCommaSeparatedPairs"] == "prm_entity=Entity1"


@patch.object(OneStream, "_get_adapter_results_data")
def test_get_agg_adapter_endpoint_data_success(mock_get_results, onestream_instance):
    """Test successful aggregated Data Adapter endpoint data retrieval."""
    mock_get_results.side_effect = [
        [{"ID": 1, "Amount": 1000}],
        [{"ID": 2, "Amount": 2000}],
    ]

    custom_subst_vars = {"prm_entity": ["E1", "E2"]}
    result = onestream_instance.get_agg_adapter_endpoint_data(
        adapter_name="TestAdapter",
        custom_subst_vars=custom_subst_vars,
    )

    assert len(result) == 2
    assert result == [[{"ID": 1, "Amount": 1000}], [{"ID": 2, "Amount": 2000}]]
    assert mock_get_results.call_count == 2


@patch.object(OneStream, "_get_adapter_results_data")
def test_get_agg_adapter_endpoint_data_no_custom_subst_vars(
    mock_get_results, onestream_instance
):
    """Test Data Adapter retrieval without custom substitution variables."""
    mock_get_results.return_value = [{"ID": 1, "Amount": 1000}]

    result = onestream_instance.get_agg_adapter_endpoint_data(
        adapter_name="TestAdapter",
    )

    assert len(result) == 1
    assert result == [[{"ID": 1, "Amount": 1000}]]
    mock_get_results.assert_called_once()


@patch.object(OneStream, "_send_api_request")
@patch.object(OneStream, "_fetch_req_results")
def test_run_sql_success(mock_fetch_results, mock_send_request, onestream_instance):
    """Test successful SQL query execution."""
    mock_response = MagicMock()
    mock_send_request.return_value = mock_response
    mock_fetch_results.return_value = SAMPLE_SQL_RESPONSE["Results"]

    result = onestream_instance._run_sql(
        sql_query="SELECT * FROM Users",
        db_location="Framework",
        results_table_name="Results",
        external_db="",
        custom_subst_vars={"prm_user": "admin"},
    )

    assert result == SAMPLE_SQL_RESPONSE["Results"]

    # Verify API call was made with correct parameters
    mock_send_request.assert_called_once()
    call_args = mock_send_request.call_args
    endpoint = call_args[0][0]
    payload_str = call_args[0][2]
    payload = json.loads(payload_str)

    assert "/api/DataProvider/GetAdoDataSetForSqlCommand" in endpoint
    assert payload["SqlQuery"] == "SELECT * FROM Users"
    assert payload["DbLocation"] == "Framework"
    assert payload["CustomSubstVarsAsCommaSeparatedPairs"] == "prm_user=admin"


@patch.object(OneStream, "_run_sql")
def test_get_agg_sql_data_success(mock_run_sql, onestream_instance):
    """Test successful aggregated SQL data retrieval."""
    mock_run_sql.side_effect = [
        [{"UserName": "admin"}],
        [{"UserName": "user1"}],
    ]

    custom_subst_vars = {"prm_role": ["Admin", "User"]}
    result = onestream_instance.get_agg_sql_data(
        sql_query="SELECT * FROM Users",
        custom_subst_vars=custom_subst_vars,
    )

    assert len(result) == 2
    assert result[0] == [{"UserName": "admin"}]
    assert result[1] == [{"UserName": "user1"}]


@patch.object(OneStream, "_run_sql")
def test_get_agg_sql_data_no_custom_subst_vars(mock_run_sql, onestream_instance):
    """Test SQL data retrieval without custom substitution variables."""
    mock_run_sql.return_value = [{"UserName": "admin"}]

    result = onestream_instance.get_agg_sql_data(
        sql_query="SELECT * FROM Users",
    )

    assert len(result) == 1
    assert result == [[{"UserName": "admin"}]]


@patch.object(OneStream, "_send_api_request")
def test_run_data_management_seq_success(mock_send_request, onestream_instance):
    """Test successful Data Management sequence execution."""
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_send_request.return_value = mock_response

    result = onestream_instance.run_data_management_seq(
        dm_seq_name="TestSequence",
        custom_subst_vars={"prm_entity": "Entity1"},
    )

    assert result == mock_response

    # Verify API call was made with correct parameters
    mock_send_request.assert_called_once()
    call_args = mock_send_request.call_args
    endpoint = call_args[0][0]
    payload_str = call_args[0][2]
    payload = json.loads(payload_str)

    assert "/api/DataManagement/ExecuteSequence" in endpoint
    assert payload["SequenceName"] == "TestSequence"
    assert payload["CustomSubstVarsAsCommaSeparatedPairs"] == "prm_entity=Entity1"


def test_to_df_success(onestream_instance):
    """Test successful DataFrame conversion."""
    data = [
        [
            {
                "ID": 1,
                "Name": "Item1",
            },
            {"ID": 2, "Name": "Item2"},
        ],
        [{"ID": 3, "Name": "Item3"}],
    ]

    df = onestream_instance.to_df(data, if_empty="fail")

    assert isinstance(df, pd.DataFrame)
    assert len(df) == 3
    assert set(df.columns) == {
        "ID",
        "Name",
        "_viadot_source",
        "_viadot_downloaded_at_utc",
    }
    assert df["ID"].tolist() == [1, 2, 3]
    assert df["Name"].tolist() == ["Item1", "Item2", "Item3"]


def test_to_df_empty_data_fail(onestream_instance):
    """Test DataFrame conversion with empty data and fail option."""
    data = []

    with pytest.raises(ValueError, match="The response data is empty"):
        onestream_instance.to_df(data, if_empty="fail")


def test_to_df_empty_data_warn(onestream_instance):
    """Test DataFrame conversion with empty data and warn option."""
    with patch.object(onestream_instance, "logger") as mock_logger:
        data = []
        df = onestream_instance.to_df(data, if_empty="warn")

        assert df.empty
        mock_logger.warning.assert_called()
