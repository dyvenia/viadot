from datetime import datetime
import json
import unittest
from unittest.mock import MagicMock, patch

import pandas as pd
from pandas.testing import assert_frame_equal
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

    def test_date_to_unix_millis(self):
        """Test Hubspot `_date_to_unix_millis` function."""
        date_str = "2021-01-01"
        expected_timestamp = int(
            datetime.strptime(date_str, "%Y-%m-%d").timestamp() * 1000
        )
        result = self.hubspot_instance._date_to_unix_millis(date_str)
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
        mock_handle_api_response.side_effect = APIError("Internal Server Error")

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
    def test_fetch_with_filters(self, mock_handle_api_response):
        """Test Hubspot `_fetch` method, with filters."""
        mock_response = MagicMock(spec=Response)
        mock_response.status_code = 200
        mock_response.json.return_value = {"results": [{"id": "123"}]}
        mock_handle_api_response.return_value = mock_response

        endpoint = "deals"
        filters = variables["filters"]
        properties = ["property1"]
        self.hubspot_instance._fetch(
            endpoint=endpoint, filters=filters, properties=properties
        )

        assert self.hubspot_instance.full_dataset is not None
        assert len(self.hubspot_instance.full_dataset) > 0

    @patch("viadot.sources.hubspot.handle_api_response")
    def test_fetch_without_filters(self, mock_handle_api_response):
        """Test Hubspot `_fetch` method, without filters."""
        mock_response = MagicMock(spec=Response)
        mock_response.status_code = 200
        mock_response.json.return_value = {"results": [{"id": "123"}]}
        mock_handle_api_response.return_value = mock_response

        endpoint = "deals"
        filters = None
        properties = ["property1"]
        self.hubspot_instance._fetch(
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
        assert_frame_equal(result_df, expected_df, check_dtype=False)
        mock_super().to_df.assert_called_once()

    @patch("viadot.sources.hubspot.super")
    def test_to_df_empty(self, mock_super):
        """Test Hubspot `to_df` method, checking emptiness."""
        mock_super().to_df = MagicMock()
        self.hubspot_instance.full_dataset = []

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

    def test_get_api_url_full_url_passthrough(self):
        """Full URL is returned unchanged."""
        url = "https://api.hubapi.com/some/path?x=1"
        assert self.hubspot_instance._get_api_url(endpoint=url) == url

    def test_get_api_url_with_filters_builds_search_url(self):
        """Relative endpoint with filters builds search URL."""
        result = self.hubspot_instance._get_api_url(endpoint="deals", filters=[{}])
        assert result.startswith(
            "https://api.hubapi.com/crm/v3/objects/deals/search/?limit=100&"
        )

    def test_get_api_url_hubdb_prefix(self):
        """HubDB endpoints are prefixed directly."""
        endpoint = "hubdb/api/v2/tables"
        expected = f"https://api.hubapi.com/{endpoint}"
        assert self.hubspot_instance._get_api_url(endpoint=endpoint) == expected

    def test_get_api_url_missing_endpoint_raises(self):
        """Missing endpoint raises ValueError."""
        with pytest.raises(ValueError, match="Endpoint must be provided."):
            self.hubspot_instance._get_api_url()

    def test_format_filters_none_returns_empty(self):
        """None filters return empty list."""
        assert self.hubspot_instance._format_filters(None) == []

    def test_extract_items_prefers_results(self):
        """_extract_items prefers 'results' key."""
        data = {"results": [{"id": "1"}], "contacts": [{"id": "x"}]}
        items = self.hubspot_instance._extract_items(data)  # type: ignore[attr-defined]
        assert items == [{"id": "1"}]

    def test_extract_items_fallback_first_list(self):
        """_extract_items falls back to first list-valued key."""
        data = {"contacts": [{"id": "x"}], "other": "val"}
        items = self.hubspot_instance._extract_items(data)  # type: ignore[attr-defined]
        assert items == [{"id": "x"}]

    def test_build_headers_contains_token(self):
        """Authorization header includes token."""
        headers = self.hubspot_instance._build_headers()  # type: ignore[attr-defined]
        assert headers["Authorization"].endswith(variables["credentials"]["token"])
        assert headers["Content-Type"] == "application/json"

    def test_fetch_pagination_with_filters(self):
        """POST pagination with 'paging.next.after' aggregates pages."""
        instance = self.hubspot_instance
        page1 = {"results": [{"id": "1"}], "paging": {"next": {"after": "abc"}}}
        page2 = {"results": [{"id": "2"}]}
        with patch.object(instance, "_api_call", side_effect=[page1, page2]):
            instance._fetch(endpoint="deals", filters=variables["filters"], nrows=10)  # type: ignore[attr-defined]
        assert instance.full_dataset == [{"id": "1"}, {"id": "2"}]

    def test_fetch_pagination_without_filters_offset(self):
        """GET pagination with 'offset' aggregates pages."""
        instance = self.hubspot_instance
        page1 = {"contacts": [{"id": "1"}], "offset": "2"}
        page2 = {"contacts": [{"id": "2"}]}
        with patch.object(instance, "_api_call", side_effect=[page1, page2]):
            instance._fetch(endpoint="contacts", filters=None, nrows=10)  # type: ignore[attr-defined]
        assert instance.full_dataset == [{"id": "1"}, {"id": "2"}]

    def test_fetch_contact_ids(self):
        """Build rows with campaign_id, contact_id, contact_type."""
        instance = self.hubspot_instance
        campaigns = ["A", "B"]
        with patch.object(
            instance, "_api_call", return_value={"results": [{"id": "c1"}, {"id": "c2"}]}
        ):
            instance._fetch_contact_ids(campaign_ids=campaigns)  # default type
        assert len(instance.full_dataset) == 4
        assert {r["campaign_id"] for r in instance.full_dataset} == set(campaigns)
        assert all("contact_id" in r for r in instance.full_dataset)
        assert all("contact_type" in r for r in instance.full_dataset)

    def test_get_campaign_metrics(self):
        """Copy selected metric keys and attach campaign_id."""
        instance = self.hubspot_instance
        with patch.object(
            instance,
            "_api_call",
            return_value={"sessions": 5, "influencedContacts": 3, "extra": "x"},
        ):
            instance._get_campaign_metrics(campaign_ids=["X", "Y"])
        assert len(instance.full_dataset) == 2
        for r in instance.full_dataset:
            assert "campaign_id" in r
            assert r["sessions"] == 5
            assert r["influencedContacts"] == 3
            assert "extra" not in r

    def test_call_api_dispatch_get_all_contacts(self):
        instance = self.hubspot_instance
        with patch.object(instance, "_fetch") as mock_fetch:
            instance.call_api(method="get_all_contacts")
            mock_fetch.assert_called_once()
            args, kwargs = mock_fetch.call_args
            assert kwargs["endpoint"] == "https://api.hubapi.com/contacts/v1/lists/all/contacts/all"

    def test_call_api_dispatch_fetch_contact_ids(self):
        instance = self.hubspot_instance
        with patch.object(instance, "_fetch_contact_ids") as mock_fc:
            instance.call_api(method="fetch_contact_ids", campaign_ids=["Z"], contact_type="influencedContacts")
            mock_fc.assert_called_once_with(campaign_ids=["Z"], contact_type="influencedContacts")

    def test_call_api_dispatch_get_campaign_metrics(self):
        instance = self.hubspot_instance
        with patch.object(instance, "_get_campaign_metrics") as mock_gcm:
            instance.call_api(method="get_campaign_metrics", campaign_ids=["Z"])
            mock_gcm.assert_called_once_with(campaign_ids=["Z"])

    def test_call_api_dispatch_generic_fetch(self):
        instance = self.hubspot_instance
        with patch.object(instance, "_fetch") as mock_fetch:
            instance.call_api(method=None, endpoint="deals", filters=None, properties=["id"], nrows=10)
            mock_fetch.assert_called_once()
            args, kwargs = mock_fetch.call_args
            assert kwargs["endpoint"] == "deals"
            assert kwargs["properties"] == ["id"]
            assert kwargs["nrows"] == 10

    @patch("viadot.sources.hubspot.super")
    def test_to_df_includes_metadata_columns(self, mock_super):
        mock_super().to_df = MagicMock()
        self.hubspot_instance.full_dataset = [{"id": "x"}]
        df = self.hubspot_instance.to_df()
        assert "_viadot_source" in df.columns
        assert "_viadot_downloaded_at_utc" in df.columns


if __name__ == "__main__":
    unittest.main()
