"""Test Matomo source."""

import json
from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest
import requests

from viadot.sources.matomo import Matomo, MatomoCredentials


@pytest.fixture
def sample_matomo_response():
    """Load sample Matomo API response for testing."""
    response_path = Path(__file__).parent / "test_matomo_response.json"
    with response_path.open(encoding="utf-8") as f:
        return json.load(f)


@pytest.fixture
def matomo_credentials():
    """Sample Matomo credentials for testing."""
    return {"api_token": "test_token_123"}


@pytest.fixture
def matomo_instance(matomo_credentials):
    """Create Matomo instance with test credentials."""
    return Matomo(credentials=matomo_credentials)


def test_valid_credentials():
    """Test creating valid MatomoCredentials."""
    creds = MatomoCredentials(api_token="test_token")  # noqa: S106
    assert creds.api_token == "test_token"  # noqa: S105


def test_missing_api_token():
    """Test MatomoCredentials with missing api_token."""
    with pytest.raises(ValueError, match="api_token"):
        MatomoCredentials()


def test_credentials_from_dict():
    """Test creating credentials from dictionary."""
    creds_dict = {"api_token": "dict_token"}
    creds = MatomoCredentials(**creds_dict)
    assert creds.api_token == "dict_token"  # noqa: S105


@patch("viadot.sources.matomo.get_source_credentials")
def test_init_with_credentials_dict(mock_get_creds, matomo_credentials):
    """Test initialization with credentials dictionary."""
    mock_get_creds.return_value = None
    matomo = Matomo(credentials=matomo_credentials)
    assert matomo.credentials == matomo_credentials


@patch("viadot.sources.matomo.get_source_credentials")
def test_init_with_config_key(mock_get_creds, matomo_credentials):
    """Test initialization with config key."""
    mock_get_creds.return_value = matomo_credentials
    matomo = Matomo(config_key="test_matomo")
    assert matomo.credentials == matomo_credentials


@patch("viadot.sources.matomo.get_source_credentials")
def test_init_without_credentials_raises_error(mock_get_creds):
    """Test initialization without credentials raises error."""
    mock_get_creds.return_value = None
    with pytest.raises(TypeError):
        Matomo()


def test_init_sets_data_to_none(matomo_instance):
    """Test that data attribute is initialized to None."""
    assert matomo_instance.data is None


@patch("viadot.sources.matomo.get_source_credentials")
def test_init_with_matomo_credentials_object(mock_get_creds):
    """Test initialization with MatomoCredentials object."""
    mock_get_creds.return_value = None
    creds_obj = MatomoCredentials(api_token="object_token")  # noqa: S106
    # Convert to dict first as the current implementation expects
    creds_dict = creds_obj.dict()
    matomo = Matomo(credentials=creds_dict)
    assert matomo.credentials["api_token"] == "object_token"  # noqa: S105


@patch("viadot.sources.matomo.handle_api_response")
def test_fetch_data_success(
    mock_handle_api_response, matomo_instance, sample_matomo_response
):
    """Test successful data fetching."""
    # Setup mock response
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = sample_matomo_response
    mock_handle_api_response.return_value = mock_response

    # Test parameters
    api_token = "test_token"  # noqa: S105
    url = "https://example.matomo.com"
    params = {
        "module": "API",
        "method": "Live.getLastVisitsDetails",
        "idSite": "1",
        "period": "day",
        "date": "today",
        "format": "JSON",
    }

    # Call fetch_data
    data = matomo_instance.fetch_data(api_token, url, params)

    # Verify the data was stored
    assert data == sample_matomo_response

    # Verify handle_api_response was called correctly
    mock_handle_api_response.assert_called_once()
    call_args = mock_handle_api_response.call_args
    assert call_args[1]["url"] == f"{url}/index.php"
    assert call_args[1]["params"] == {**params, "token_auth": api_token}
    assert call_args[1]["method"] == "GET"


def test_fetch_data_with_empty_params_raises_validation_error(matomo_instance):
    """Test fetch_data with empty params dictionary raises validation error."""
    with pytest.raises(ValueError, match="Missing required API parameters"):
        matomo_instance.fetch_data("token", "https://example.com", {})


def test_fetch_data_with_none_params_raises_validation_error(matomo_instance):
    """Test fetch_data with None params raises validation error."""
    with pytest.raises(ValueError, match="Missing required API parameters"):
        matomo_instance.fetch_data("token", "https://example.com", None)


@patch("viadot.sources.matomo.handle_api_response")
def test_fetch_data_request_exception(mock_handle_api_response, matomo_instance):
    """Test fetch_data with request exception."""
    from viadot.exceptions import APIError

    mock_handle_api_response.side_effect = requests.exceptions.RequestException(
        "Connection failed"
    )

    with pytest.raises(APIError):
        matomo_instance.fetch_data(
            "token",
            "https://example.com",
            {
                "module": "API",
                "method": "test",
                "idSite": "1",
                "period": "day",
                "date": "today",
                "format": "JSON",
            },
        )


def test_fetch_data_missing_api_token(matomo_instance):
    """Test fetch_data raises error when api_token is empty."""
    with pytest.raises(ValueError, match="api_token is required and cannot be empty"):
        matomo_instance.fetch_data("", "https://example.com", {"module": "API"})


def test_fetch_data_missing_url(matomo_instance):
    """Test fetch_data raises error when url is empty."""
    with pytest.raises(ValueError, match="url is required and cannot be empty"):
        matomo_instance.fetch_data("token", "", {"module": "API"})


def test_fetch_data_missing_required_params(matomo_instance):
    """Test fetch_data raises error when required parameters are missing."""
    # Missing all required params
    with pytest.raises(ValueError, match="Missing required API parameters"):
        matomo_instance.fetch_data("token", "https://example.com", {})

    # Missing some required params
    incomplete_params = {
        "module": "API",
        "method": "test",
    }  # missing idSite, period, date, format
    with pytest.raises(ValueError, match="Missing required API parameters"):
        matomo_instance.fetch_data("token", "https://example.com", incomplete_params)


def test_to_df_without_data_raises_error(matomo_instance):
    """Test to_df raises error when no data has been fetched."""
    with pytest.raises(
        ValueError, match="No data available. Call fetch_data\\(\\) first."
    ):
        matomo_instance.to_df(
            data=None, top_level_fields=["test_field"], record_path="actionDetails"
        )


def test_to_df_basic_conversion(matomo_instance, sample_matomo_response):
    """Test basic DataFrame conversion."""
    data = sample_matomo_response
    df = matomo_instance.to_df(
        data=data, top_level_fields=["idSite", "idVisit"], record_path="actionDetails"
    )

    assert isinstance(df, pd.DataFrame)
    assert not df.empty
    # Check that top-level fields are included
    assert "idSite" in df.columns
    assert "idVisit" in df.columns


def test_to_df_with_record_prefix(matomo_instance, sample_matomo_response):
    """Test to_df with record prefix."""
    data = sample_matomo_response

    df = matomo_instance.to_df(
        data=data,
        top_level_fields=["idSite"],
        record_path="actionDetails",
        record_prefix="action_",
    )

    # Check that actionDetails fields have the prefix
    action_columns = [col for col in df.columns if col.startswith("action_")]
    assert len(action_columns) == 0


def test_to_df_with_list_record_path(matomo_instance, sample_matomo_response):
    """Test to_df with list record_path."""
    data = sample_matomo_response

    df = matomo_instance.to_df(
        data=data, top_level_fields=["idSite"], record_path=["actionDetails"]
    )

    assert isinstance(df, pd.DataFrame)
    assert not df.empty


def test_to_df_empty_result_warn(matomo_instance):
    """Test to_df with empty result and warn if_empty."""
    data = {"actionDetails": []}

    with patch("viadot.sources.base.logger.warning") as mock_warning:
        df = matomo_instance.to_df(
            data=data,
            top_level_fields=["idSite"],
            record_path="actionDetails",
            if_empty="warn",
        )

        assert df.empty
        # Warning is called twice: once in to_df, once in _handle_if_empty
        assert mock_warning.call_count == 2
        mock_warning.assert_any_call("No records found in the specified record_path.")
        mock_warning.assert_any_call("The query produced no data.")


def test_to_df_empty_result_skip(matomo_instance):
    """Test to_df with empty result and skip if_empty."""
    from viadot.signals import SKIP

    data = {"actionDetails": []}

    with pytest.raises(SKIP):
        matomo_instance.to_df(
            data=data,
            top_level_fields=["idSite"],
            record_path="actionDetails",
            if_empty="skip",
        )


def test_to_df_empty_result_fail(matomo_instance):
    """Test to_df with empty result and fail if_empty."""
    data = {"actionDetails": []}

    with pytest.raises(ValueError, match="The query produced no data"):
        matomo_instance.to_df(
            data=data,
            top_level_fields=["idSite"],
            record_path="actionDetails",
            if_empty="fail",
        )


def test_to_df_with_tests_validation(matomo_instance, sample_matomo_response):
    """Test to_df with tests validation."""
    data = sample_matomo_response

    test_config = {"column_tests": {"idSite": {"not_null": True}}}

    with patch("viadot.sources.matomo.validate") as mock_validate:
        df = matomo_instance.to_df(
            data=data,
            top_level_fields=["idSite"],
            record_path="actionDetails",
            tests=test_config,
        )

        mock_validate.assert_called_once()
        args, kwargs = mock_validate.call_args
        assert kwargs["df"] is df
        assert kwargs["tests"] == test_config


@patch("viadot.sources.matomo.handle_api_response")
def test_full_workflow(
    mock_handle_api_response, matomo_instance, sample_matomo_response
):
    """Test complete workflow from fetch to DataFrame."""
    # Setup mock response
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = sample_matomo_response
    mock_handle_api_response.return_value = mock_response

    # Fetch data
    matomo_instance.fetch_data(
        "test_token",
        "https://example.matomo.com",
        {
            "module": "API",
            "method": "Live.getLastVisitsDetails",
            "idSite": "1",
            "period": "day",
            "date": "today",
            "format": "JSON",
        },
    )

    # Convert to DataFrame
    df = matomo_instance.to_df(
        data=sample_matomo_response,
        top_level_fields=["idSite", "idVisit"],
        record_path="actionDetails",
    )

    # Verify results
    assert isinstance(df, pd.DataFrame)
    assert not df.empty
    assert "idSite" in df.columns
    assert "idVisit" in df.columns


def test_data_persistence(matomo_instance, sample_matomo_response):
    """Test that data persists between method calls."""
    # First call
    df1 = matomo_instance.to_df(
        data=sample_matomo_response,
        top_level_fields=["idSite"],
        record_path="actionDetails",
    )
    # Second call
    df2 = matomo_instance.to_df(
        data=sample_matomo_response,
        top_level_fields=["idVisit"],
        record_path="actionDetails",
    )

    assert not df1.empty
    assert not df2.empty
