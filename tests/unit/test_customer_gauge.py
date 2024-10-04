"""'test_customer_gauge.py'."""

from unittest.mock import MagicMock, patch

import pytest

from viadot.exceptions import APIError
from viadot.sources.customer_gauge import CustomerGauge, CustomerGaugeCredentials


variables = {
    "credentials": {
        "client_id": "client_id",
        "client_secret": "client_secret",  # pragma: allowlist secret
    }
}


@pytest.mark.basic
def test_customer_gauge_credentials():
    """Test Customer Gauge credentials."""
    CustomerGaugeCredentials(
        client_id=variables["credentials"]["client_id"],
        client_secret=variables["credentials"]["client_secret"],
    )


@pytest.mark.basic
def test_missing_credentials():
    """Test raise error without Customer Gauge credentials."""
    with pytest.raises(TypeError):
        CustomerGauge(config_key="invalid_key")


@pytest.mark.connect
@patch("viadot.sources.customer_gauge.handle_api_response")
def test_get_token_success(mock_handle_api_response):
    """Test Customer Gauge get token."""
    mock_response = MagicMock()
    mock_response.json.return_value = {"access_token": "fake_token"}
    mock_handle_api_response.return_value = mock_response

    customer_gauge_instance = CustomerGauge(credentials=variables["credentials"])

    token = customer_gauge_instance._get_token()
    assert token == "fake_token"  # noqa: S105 # pragma: allowlist secret
    mock_handle_api_response.assert_called_once()


@pytest.mark.connect
@patch("viadot.sources.customer_gauge.handle_api_response")
def test_get_token_failure(mock_handle_api_response):
    """Test Customer Gauge get token failure."""
    mock_response = MagicMock()
    mock_response.json.return_value = {"access_token": None}
    mock_handle_api_response.return_value = mock_response

    customer_gauge_instance = CustomerGauge(credentials=variables["credentials"])

    with pytest.raises(
        APIError, match="The token could not be generated. Check your credentials."
    ):
        customer_gauge_instance._get_token()


@pytest.mark.connect
@patch(
    "viadot.sources.customer_gauge.CustomerGauge._get_token", return_value="fake_token"
)
@patch("viadot.sources.customer_gauge.handle_api_response")
def test_get_json_response(mock_handle_api_response, mock_get_token):  # noqa: ARG001
    """Test Customer Gauge json response."""
    mock_response = MagicMock()
    mock_response.json.return_value = {"data": [], "cursor": {"next": 1}}
    mock_handle_api_response.return_value = mock_response

    customer_gauge_instance = CustomerGauge(credentials=variables["credentials"])

    json_response = customer_gauge_instance._get_json_response(url="fake_url")
    assert "data" in json_response


@pytest.mark.connect
def test_get_cursor():
    """Test Customer Gauge get cursor."""
    json_response = {"cursor": {"next": 10}}
    customer_gauge_instance = CustomerGauge(credentials=variables["credentials"])

    cursor = customer_gauge_instance._get_cursor(json_response)
    assert cursor == 10


@pytest.mark.connect
def test_get_cursor_key_error():
    """Test Customer Gauge get cursor error."""
    json_response = {"no_cursor": {}}
    customer_gauge_instance = CustomerGauge(credentials=variables["credentials"])

    with pytest.raises(
        ValueError, match="Provided argument doesn't contain 'cursor' value."
    ):
        customer_gauge_instance._get_cursor(json_response)


@pytest.mark.functions
def test_column_unpacker():
    """Test Customer Gauge function `_unpack_columns`."""
    json_list = [{"field": {"key1": "value1", "key2": "value2"}}]
    customer_gauge_instance = CustomerGauge(credentials=variables["credentials"])

    unpacked_list = customer_gauge_instance._unpack_columns(
        json_list=json_list, unpack_by_field_reference_cols=["field"]
    )
    assert "key1" in unpacked_list[0]["field"]
