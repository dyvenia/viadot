"""Test ECB source."""

from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest
import requests

from viadot.sources.ecb import ECB


@pytest.fixture
def sample_ecb_response():
    """Load sample ECB API response for testing."""
    response_path = Path(__file__).parent / "test_ecb_response.xml"
    with response_path.open(encoding="utf-8") as f:
        return f.read()


@pytest.fixture
def ecb_instance():
    """Create ECB instance."""
    return ECB()


def test_init(ecb_instance):
    """Test ECB initialization."""
    assert ecb_instance.credentials is None
    assert ecb_instance.data is None


@patch("viadot.sources.ecb.handle_api_response")
def test_fetch_data_success(
    mock_handle_api_response, ecb_instance, sample_ecb_response
):
    """Test successful data fetching."""
    # Setup mock response
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.text = sample_ecb_response
    mock_handle_api_response.return_value = mock_response

    # Test parameters
    url = "https://www.ecb.europa.eu/stats/eurofxref/eurofxref-daily.xml"

    # Call fetch_data
    data = ecb_instance.fetch_data(url)

    # Verify the data was returned
    assert data == sample_ecb_response

    # Verify handle_api_response was called correctly
    mock_handle_api_response.assert_called_once()
    call_args = mock_handle_api_response.call_args
    assert call_args[1]["url"] == url
    assert call_args[1]["params"] is None
    assert call_args[1]["method"] == "GET"


def test_fetch_data_missing_url(ecb_instance):
    """Test fetch_data raises error when url is empty."""
    with pytest.raises(ValueError, match="url is required and cannot be empty"):
        ecb_instance.fetch_data("")


@patch("viadot.sources.ecb.handle_api_response")
def test_fetch_data_request_exception(mock_handle_api_response, ecb_instance):
    """Test fetch_data with request exception."""
    from viadot.exceptions import APIError

    mock_handle_api_response.side_effect = requests.exceptions.RequestException(
        "Connection failed"
    )

    with pytest.raises(APIError):
        ecb_instance.fetch_data(
            "https://www.ecb.europa.eu/stats/eurofxref/eurofxref-daily.xml"
        )


def test_parse_xml(ecb_instance, sample_ecb_response):
    """Test XML parsing."""
    df = ecb_instance._parse_xml(sample_ecb_response)

    assert isinstance(df, pd.DataFrame)
    assert not df.empty
    assert "time" in df.columns
    assert "currency" in df.columns
    assert "rate" in df.columns

    # Check that we have data
    assert len(df) > 0

    # Check that time is consistent
    assert df["time"].nunique() == 1
    assert df["time"].iloc[0] == "2025-12-30"

    # Check that currency values are strings
    assert df["currency"].dtype == "object"

    # Check that rate values are numeric
    assert pd.api.types.is_numeric_dtype(df["rate"])


def test_parse_xml_invalid_xml(ecb_instance):
    """Test parse_xml with invalid XML."""
    # Use XML that doesn't match ECB structure
    # This will parse successfully but fail structure validation
    invalid_xml = "<invalid>xml</invalid>"
    with pytest.raises(ValueError, match="Could not find time attribute"):
        ecb_instance._parse_xml(invalid_xml)


def test_parse_xml_missing_time(ecb_instance):
    """Test parse_xml with XML missing time attribute."""
    xml_without_time = """<?xml version="1.0" encoding="UTF-8"?>
    <gesmes:Envelope xmlns:gesmes="http://www.gesmes.org/xml/2002-08-01" xmlns="http://www.ecb.int/vocabulary/2002-08-01/eurofxref">
        <Cube>
            <Cube currency='USD' rate='1.1757'/>
        </Cube>
    </gesmes:Envelope>"""
    with pytest.raises(ValueError, match="Could not find time attribute"):
        ecb_instance._parse_xml(xml_without_time)


def test_parse_xml_empty_data(ecb_instance):
    """Test parse_xml with XML containing no exchange rates."""
    xml_empty = """<?xml version="1.0" encoding="UTF-8"?>
    <gesmes:Envelope xmlns:gesmes="http://www.gesmes.org/xml/2002-08-01" xmlns="http://www.ecb.int/vocabulary/2002-08-01/eurofxref">
        <Cube>
            <Cube time='2025-12-30'>
            </Cube>
        </Cube>
    </gesmes:Envelope>"""
    df = ecb_instance._parse_xml(xml_empty)
    assert df.empty
    assert list(df.columns) == ["time", "currency", "rate"]


@patch("viadot.sources.ecb.handle_api_response")
def test_to_df_success(mock_handle_api_response, ecb_instance, sample_ecb_response):
    """Test successful to_df conversion."""
    # Setup mock response
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.text = sample_ecb_response
    mock_handle_api_response.return_value = mock_response

    url = "https://www.ecb.europa.eu/stats/eurofxref/eurofxref-daily.xml"

    df = ecb_instance.to_df(url=url)

    assert isinstance(df, pd.DataFrame)
    assert not df.empty
    assert "time" in df.columns
    assert "currency" in df.columns
    assert "rate" in df.columns


def test_to_df_missing_url(ecb_instance):
    """Test to_df raises error when url is empty."""
    with pytest.raises(ValueError, match="url is required and cannot be empty"):
        ecb_instance.to_df(url="")


@patch("viadot.sources.ecb.handle_api_response")
def test_to_df_empty_result_warn(mock_handle_api_response, ecb_instance):
    """Test to_df with empty result and warn if_empty."""
    xml_empty = """<?xml version="1.0" encoding="UTF-8"?>
    <gesmes:Envelope xmlns:gesmes="http://www.gesmes.org/xml/2002-08-01" xmlns="http://www.ecb.int/vocabulary/2002-08-01/eurofxref">
        <Cube>
            <Cube time='2025-12-30'>
            </Cube>
        </Cube>
    </gesmes:Envelope>"""

    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.text = xml_empty
    mock_handle_api_response.return_value = mock_response

    with patch("viadot.sources.base.logger.warning") as mock_warning:
        df = ecb_instance.to_df(
            url="https://www.ecb.europa.eu/stats/eurofxref/eurofxref-daily.xml",
            if_empty="warn",
        )

        assert df.empty

        assert mock_warning.call_count == 3
        mock_warning.assert_any_call("No exchange rate data found in XML response.")
        mock_warning.assert_any_call("No exchange rates found in the response.")
        mock_warning.assert_any_call("The query produced no data.")


@patch("viadot.sources.ecb.handle_api_response")
def test_to_df_empty_result_skip(mock_handle_api_response, ecb_instance):
    """Test to_df with empty result and skip if_empty."""
    from viadot.signals import SKIP

    xml_empty = """<?xml version="1.0" encoding="UTF-8"?>
    <gesmes:Envelope xmlns:gesmes="http://www.gesmes.org/xml/2002-08-01" xmlns="http://www.ecb.int/vocabulary/2002-08-01/eurofxref">
        <Cube>
            <Cube time='2025-12-30'>
            </Cube>
        </Cube>
    </gesmes:Envelope>"""

    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.text = xml_empty
    mock_handle_api_response.return_value = mock_response

    with pytest.raises(SKIP):
        ecb_instance.to_df(
            url="https://www.ecb.europa.eu/stats/eurofxref/eurofxref-daily.xml",
            if_empty="skip",
        )


@patch("viadot.sources.ecb.handle_api_response")
def test_to_df_empty_result_fail(mock_handle_api_response, ecb_instance):
    """Test to_df with empty result and fail if_empty."""
    xml_empty = """<?xml version="1.0" encoding="UTF-8"?>
    <gesmes:Envelope xmlns:gesmes="http://www.gesmes.org/xml/2002-08-01" xmlns="http://www.ecb.int/vocabulary/2002-08-01/eurofxref">
        <Cube>
            <Cube time='2025-12-30'>
            </Cube>
        </Cube>
    </gesmes:Envelope>"""

    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.text = xml_empty
    mock_handle_api_response.return_value = mock_response

    with pytest.raises(ValueError, match="The query produced no data"):
        ecb_instance.to_df(
            url="https://www.ecb.europa.eu/stats/eurofxref/eurofxref-daily.xml",
            if_empty="fail",
        )


@patch("viadot.sources.ecb.handle_api_response")
def test_to_df_with_tests_validation(
    mock_handle_api_response, ecb_instance, sample_ecb_response
):
    """Test to_df with tests validation."""
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.text = sample_ecb_response
    mock_handle_api_response.return_value = mock_response

    test_config = {"column_tests": {"currency": {"not_null": True}}}

    with patch("viadot.sources.ecb.validate") as mock_validate:
        df = ecb_instance.to_df(
            url="https://www.ecb.europa.eu/stats/eurofxref/eurofxref-daily.xml",
            tests=test_config,
        )

        mock_validate.assert_called_once()
        args, kwargs = mock_validate.call_args
        assert kwargs["df"] is df
        assert kwargs["tests"] == test_config


@patch("viadot.sources.ecb.handle_api_response")
def test_full_workflow(mock_handle_api_response, ecb_instance, sample_ecb_response):
    """Test complete workflow from fetch to DataFrame."""
    # Setup mock response
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.text = sample_ecb_response
    mock_handle_api_response.return_value = mock_response

    url = "https://www.ecb.europa.eu/stats/eurofxref/eurofxref-daily.xml"

    # Fetch and convert to DataFrame
    df = ecb_instance.to_df(url=url)

    # Verify results
    assert isinstance(df, pd.DataFrame)
    assert not df.empty
    assert "time" in df.columns
    assert "currency" in df.columns
    assert "rate" in df.columns

    # Verify we have multiple currencies
    assert df["currency"].nunique() > 1
