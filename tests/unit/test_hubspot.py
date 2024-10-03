from datetime import datetime
from io import StringIO
import json
import unittest
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest
from requests.models import Response

from viadot.exceptions import APIError
from viadot.sources import Hubspot
from viadot.sources.hubspot import HubspotCredentials


variables = {
    "credentials": {"token": "fake_token"},
    "filters": [
        {
            "filters": [
                {
                    "propertyName": "createdate",
                    "operator": "GTE",
                    "value": "2021-01-01",
                }
            ]
        }
    ],
}


class TestHubspotCredentials:
    """Test Hubspot Credentials Class."""

    def test_hubspot_credentials(self):
        """Test Hubspot credentials."""
        HubspotCredentials(token="test_token")  # noqa: S106


class TestHubspot(unittest.TestCase):
    """Test Hubspot Class."""

    @classmethod
    def setUpClass(cls):  # noqa: ANN206
        """Defined based Hubspot Class for the rest of test."""
        cls.hubspot_instance = Hubspot(credentials=variables["credentials"])

    @patch("viadot.sources.hubspot.get_source_credentials", return_value=None)
    def test_init_no_credentials(self, mock_get_source_credentials):
        """Test raise error without credentials."""
        with pytest.raises(TypeError):
            Hubspot()

        mock_get_source_credentials.assert_called_once()

    def test_date_to_unixtimestamp(self):
        """Test Hubspot `_date_to_unixtimestamp` function."""
        date_str = "2021-01-01"
        expected_timestamp = int(
            datetime.strptime(date_str, "%Y-%m-%d").timestamp() * 1000
        )
        result = self.hubspot_instance._date_to_unixtimestamp(date_str)
        assert result == expected_timestamp

    def test_get_api_url(self):
        """Test Hubspot `_get_api_url` function."""
        endpoint = "deals"
        filters = None
        properties = ["property1", "property2"]
        expected_url = (
            f"https://api.hubapi.com/crm/v3/objects/{endpoint}/"
            + "?limit=100&properties=property1,property2&"
        )
        result = self.hubspot_instance._get_api_url(
            endpoint=endpoint, filters=filters, properties=properties
        )
        assert result == expected_url

    def test_format_filters(self):
        """Test Hubspot `_format_filters` function."""
        filters = variables["filters"]
        formatted_filters = self.hubspot_instance._format_filters(filters)
        assert isinstance(formatted_filters, list)

    def test_get_api_body(self):
        """Test Hubspot `_get_api_body` function."""
        filters = variables["filters"]
        expected_body = json.dumps({"filterGroups": filters, "limit": 100})
        result = self.hubspot_instance._get_api_body(filters)

        assert result == expected_body

    @patch("viadot.sources.hubspot.handle_api_response")
    def test_api_call_success(self, mock_handle_api_response):
        """Test Hubspot `_api_call` method."""
        mock_response = MagicMock(spec=Response)
        mock_response.status_code = 200
        mock_response.json.return_value = {"results": [{"id": "123"}]}
        mock_handle_api_response.return_value = mock_response

        url = "https://api.hubapi.com/crm/v3/objects/deals/?limit=100&"
        result = self.hubspot_instance._api_call(url=url, method="GET")

        assert result == {"results": [{"id": "123"}]}
        mock_handle_api_response.assert_called_once()

    @patch("viadot.sources.hubspot.handle_api_response")
    def test_api_call_error(self, mock_handle_api_response):
        """Test Hubspot `_api_call` method failure."""
        mock_response = MagicMock(spec=Response)
        mock_response.status_code = 500
        mock_response.content = b"Internal Server Error"
        mock_handle_api_response.return_value = mock_response

        url = "https://api.hubapi.com/crm/v3/objects/deals/?limit=100&"

        with pytest.raises(APIError):
            self.hubspot_instance._api_call(url=url, method="GET")

    def test_get_offset_from_response(self):
        """Test Hubspot `_get_offset_from_response` function."""
        response_with_paging = {"paging": {"next": {"after": "123"}}}
        response_with_offset = {"offset": "456"}

        offset_type, offset_value = self.hubspot_instance._get_offset_from_response(
            response_with_paging
        )
        assert (offset_type, offset_value) == ("after", "123")

        offset_type, offset_value = self.hubspot_instance._get_offset_from_response(
            response_with_offset
        )
        assert (offset_type, offset_value) == ("offset", "456")

        offset_type, offset_value = self.hubspot_instance._get_offset_from_response({})
        assert (offset_type, offset_value) == (None, None)

    @patch("viadot.sources.hubspot.handle_api_response")
    def test_api_connection_with_filters(self, mock_handle_api_response):
        """Test Hubspot `api_connection` method, with filters."""
        mock_response = MagicMock(spec=Response)
        mock_response.status_code = 200
        mock_response.json.return_value = {"results": [{"id": "123"}]}
        mock_handle_api_response.return_value = mock_response

        endpoint = "deals"
        filters = variables["filters"]
        properties = ["property1"]
        self.hubspot_instance.api_connection(
            endpoint=endpoint, filters=filters, properties=properties
        )

        assert self.hubspot_instance.full_dataset is not None
        assert len(self.hubspot_instance.full_dataset) > 0

    @patch("viadot.sources.hubspot.handle_api_response")
    def test_api_connection_without_filters(self, mock_handle_api_response):
        """Test Hubspot `api_connection` method, without filters."""
        mock_response = MagicMock(spec=Response)
        mock_response.status_code = 200
        mock_response.json.return_value = {"results": [{"id": "123"}]}
        mock_handle_api_response.return_value = mock_response

        endpoint = "deals"
        filters = None
        properties = ["property1"]
        self.hubspot_instance.api_connection(
            endpoint=endpoint, filters=filters, properties=properties
        )

        assert self.hubspot_instance.full_dataset is not None
        assert len(self.hubspot_instance.full_dataset) > 0

    @patch("viadot.sources.hubspot.super")
    def test_to_df(self, mock_super):
        """Test Hubspot `to_df` function."""
        mock_super().to_df = MagicMock()
        self.hubspot_instance.full_dataset = [{"id": "123"}]
        result_df = self.hubspot_instance.to_df()
        result_df.drop(
            columns=["_viadot_source", "_viadot_downloaded_at_utc"],
            inplace=True,
            axis=1,
        )

        expected_df = pd.DataFrame([{"id": "123"}])
        assert result_df.equals(expected_df)
        mock_super().to_df.assert_called_once()

    @patch("viadot.sources.hubspot.pd.read_json")
    @patch("viadot.sources.hubspot.super")
    def test_to_df_empty(self, mock_super, mock_read_json):
        """Test Hubspot `to_df` method, checking emptiness."""
        mock_super().to_df = MagicMock()
        mock_read_json.return_value = pd.DataFrame()
        self.hubspot_instance.full_dataset = StringIO("{}")

        with patch.object(
            self.hubspot_instance, "_handle_if_empty"
        ) as mock_handle_if_empty:
            result_df = self.hubspot_instance.to_df()
            result_df.drop(
                columns=["_viadot_source", "_viadot_downloaded_at_utc"],
                inplace=True,
                axis=1,
            )
            mock_handle_if_empty.assert_called_once_with(
                if_empty="warn", message="The response does not contain any data."
            )
            assert result_df.empty
            mock_super().to_df.assert_called_once()


if __name__ == "__main__":
    unittest.main()
