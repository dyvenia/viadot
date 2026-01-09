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
        result = self.hubspot_instance._get_request_body(filters)

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
        result = self.hubspot_instance._fetch(
            endpoint=endpoint, filters=filters, properties=properties
        )

        assert result is not None
        assert len(result) > 0

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
        result = self.hubspot_instance._fetch(
            endpoint=endpoint, filters=filters, properties=properties
        )

        assert result is not None
        assert len(result) > 0

    @patch("viadot.sources.base.Source.to_df")
    def test_to_df(self, mock_parent_to_df):
        """Test Hubspot `to_df` function."""
        mock_parent_to_df.return_value = pd.DataFrame()
        result_df = self.hubspot_instance.to_df(data=[{"id": "123"}])
        result_df.drop(
            columns=["_viadot_source", "_viadot_downloaded_at_utc"],
            inplace=True,
            axis=1,
        )

        expected_df = pd.DataFrame([{"id": "123"}])
        assert_frame_equal(result_df, expected_df, check_dtype=False)
        mock_parent_to_df.assert_called_once()

    @patch("viadot.sources.base.Source.to_df")
    def test_to_df_empty(self, mock_parent_to_df):
        """Test Hubspot `to_df` method, checking emptiness."""
        mock_parent_to_df.return_value = pd.DataFrame()

        with patch.object(
            self.hubspot_instance, "_handle_if_empty"
        ) as mock_handle_if_empty:
            result_df = self.hubspot_instance.to_df(data=[])
            result_df.drop(
                columns=["_viadot_source", "_viadot_downloaded_at_utc"],
                inplace=True,
                axis=1,
            )
            mock_handle_if_empty.assert_called_once_with(
                if_empty="warn", message="The response does not contain any data."
            )
            assert result_df.empty
            mock_parent_to_df.assert_called_once()

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
            result = instance._fetch(
                endpoint="deals", filters=variables["filters"], nrows=10
            )  # type: ignore[attr-defined]
        assert len(result) == 2
        assert result[0]["id"] == "1"
        assert result[1]["id"] == "2"

    def test_fetch_pagination_without_filters_offset(self):
        """GET pagination with 'offset' aggregates pages."""
        instance = self.hubspot_instance
        page1 = {"contacts": [{"id": "1"}], "offset": "2"}
        page2 = {"contacts": [{"id": "2"}]}
        with patch.object(instance, "_api_call", side_effect=[page1, page2]):
            result = instance._fetch(endpoint="contacts", filters=None, nrows=10)  # type: ignore[attr-defined]
        assert len(result) == 2
        assert result[0]["id"] == "1"
        assert result[1]["id"] == "2"

    def test_fetch_contact_ids(self):
        """Build rows with campaign_id, contact_id, contact_type."""
        instance = self.hubspot_instance
        campaigns = ["A", "B"]
        with patch.object(
            instance,
            "_api_call",
            return_value={"results": [{"id": "c1"}, {"id": "c2"}]},
        ):
            result = instance._fetch_contact_ids(campaign_ids=campaigns)  # default type
        assert len(result) == 4
        assert {r["campaign_id"] for r in result} == set(campaigns)
        assert all("contact_id" in r for r in result)
        assert all("contact_type" in r for r in result)

    def test_get_campaign_budget_totals(self):
        """Keep only totals and campaign_id."""
        instance = self.hubspot_instance
        responses = [
            {
                "budgetItems": [{"amount": 1}],
                "spendItems": [{"amount": 2}],
                "currencyCode": "USD",
                "budgetTotal": 100,
                "remainingBudget": 60,
                "spendTotal": 40,
            },
            {
                "budgetItems": [],
                "spendItems": [],
                "currencyCode": "EUR",
                "budgetTotal": 200,
                "remainingBudget": 150,
                "spendTotal": 50,
            },
        ]
        with patch.object(instance, "_api_call", side_effect=responses):
            result = instance._get_campaign_budget_totals(campaign_ids=["X", "Y"])  # type: ignore[attr-defined]
        assert len(result) == 2
        row0 = result[0]
        assert row0["campaign_id"] == "X"
        assert row0["budgetTotal"] == 100
        assert row0["remainingBudget"] == 60
        assert row0["spendTotal"] == 40
        assert row0["currencyCode"] == "USD"
        row1 = result[1]
        assert row1["campaign_id"] == "Y"
        assert row1["budgetTotal"] == 200
        assert row1["remainingBudget"] == 150
        assert row1["spendTotal"] == 50
        assert row1["currencyCode"] == "EUR"

    def test_call_api_dispatch_get_campaign_budget_totals(self):
        instance = self.hubspot_instance
        with patch.object(instance, "_get_campaign_budget_totals") as mock_gbt:
            instance.call_api(method="get_campaign_budget_totals", campaign_ids=["Z"])
            mock_gbt.assert_called_once_with(campaign_ids=["Z"])

    def test_get_campaign_metrics(self):
        """Copy selected metric keys and attach campaign_id."""
        instance = self.hubspot_instance
        with patch.object(
            instance,
            "_api_call",
            return_value={"sessions": 5, "influencedContacts": 3, "extra": "x"},
        ):
            result = instance._get_campaign_metrics(campaign_ids=["X", "Y"])
        assert len(result) == 2
        for r in result:
            assert "campaign_id" in r
            assert r["sessions"] == 5
            assert r["influencedContacts"] == 3
            assert "extra" not in r

    def test_get_campaign_details_defaults(self):
        """Fetch campaign details with default properties."""
        instance = self.hubspot_instance
        responses = [
            {
                "properties": {
                    "hs_name": "Camp A",
                    "hs_start_date": "2021-01-01",
                    "hs_end_date": "2021-02-01",
                    "hs_notes": "Note",
                    "hs_owner": "Owner",
                }
            },
            {
                "properties": {
                    "hs_name": "Camp B",
                    "hs_start_date": "2021-03-01",
                    "hs_end_date": "2021-04-01",
                    "hs_notes": "Note",
                    "hs_owner": "Owner",
                }
            },
        ]
        with patch.object(instance, "_api_call", side_effect=responses):
            result = instance._get_campaign_details(campaign_ids=["X", "Y"])  # type: ignore[attr-defined]
        assert len(result) == 2
        for row in result:
            assert "campaign_id" in row
            assert "hs_name" in row
            assert "hs_start_date" in row
            assert "hs_end_date" in row
            assert "hs_notes" in row
            assert "hs_owner" in row

    def test_get_campaign_details_empty_properties(self):
        """Handles empty 'properties' gracefully (only campaign_id present)."""
        instance = self.hubspot_instance
        responses = [{"properties": {}}, {}]
        with patch.object(instance, "_api_call", side_effect=responses):
            result = instance._get_campaign_details(campaign_ids=["X", "Y"])  # type: ignore[attr-defined]
        assert len(result) == 2
        for row, cid in zip(result, ["X", "Y"]):
            assert row["campaign_id"] == cid
            # All props keys should be present with None values
            assert "hs_name" in row
            assert row["hs_name"] is None
            assert "hs_start_date" in row
            assert row["hs_start_date"] is None
            assert "hs_end_date" in row
            assert row["hs_end_date"] is None
            assert "hs_notes" in row
            assert row["hs_notes"] is None
            assert "hs_owner" in row
            assert row["hs_owner"] is None

    def test_call_api_dispatch_get_campaign_details(self):
        instance = self.hubspot_instance
        with patch.object(instance, "_get_campaign_details") as mock_gcd:
            instance.call_api(method="get_campaign_details", campaign_ids=["Z"])
            mock_gcd.assert_called_once_with(campaign_ids=["Z"])

    def test_call_api_dispatch_get_all_contacts(self):
        instance = self.hubspot_instance
        with patch.object(instance, "_fetch") as mock_fetch:
            instance.call_api(method="get_all_contacts")
            mock_fetch.assert_called_once()
            args, kwargs = mock_fetch.call_args
            assert (
                kwargs["endpoint"]
                == "https://api.hubapi.com/contacts/v1/lists/all/contacts/all"
            )

    def test_call_api_dispatch_fetch_contact_ids(self):
        instance = self.hubspot_instance
        with patch.object(instance, "_fetch_contact_ids") as mock_fc:
            instance.call_api(
                method="fetch_contact_ids",
                campaign_ids=["Z"],
                contact_type="influencedContacts",
            )
            mock_fc.assert_called_once_with(
                campaign_ids=["Z"], contact_type="influencedContacts"
            )

    def test_call_api_dispatch_get_campaign_metrics(self):
        instance = self.hubspot_instance
        with patch.object(instance, "_get_campaign_metrics") as mock_gcm:
            instance.call_api(method="get_campaign_metrics", campaign_ids=["Z"])
            mock_gcm.assert_called_once_with(campaign_ids=["Z"])

    def test_call_api_dispatch_generic_fetch(self):
        instance = self.hubspot_instance
        with patch.object(instance, "_fetch") as mock_fetch:
            instance.call_api(
                method=None, endpoint="deals", filters=None, properties=["id"], nrows=10
            )
            mock_fetch.assert_called_once()
            args, kwargs = mock_fetch.call_args
            assert kwargs["endpoint"] == "deals"
            assert kwargs["properties"] == ["id"]
            assert kwargs["nrows"] == 10

    def test_call_api_expands_stringified_json_in_form_submissions(self):
        instance = self.hubspot_instance
        payload = {
            "conversion-id": "62775177-4616-413b-9c26-bb35057913c4",
            "timestamp": 1613053768706,
            "form-id": "2a9da523-cb9a-4e1f-a4b0-2ac746a855ac",
        }
        rows = [{"id": "1", "form_submissions": json.dumps(payload)}]
        with patch.object(instance, "_fetch", return_value=rows):
            result = instance.call_api(method=None, endpoint="deals", filters=None)  # type: ignore[arg-type]
        row = result[0]
        assert row["form_submissions_json_conversion-id"] == payload["conversion-id"]
        assert row["form_submissions_json_timestamp"] == payload["timestamp"]
        assert row["form_submissions_json_form-id"] == payload["form-id"]
        # original column stays
        assert "form_submissions" in row

    def test_call_api_expands_single_item_list_of_dicts_in_identity_profiles(self):
        instance = self.hubspot_instance
        list_payload = [
            {
                "vid": 3351,
                "saved-at-timestamp": 1613053343363,
                "deleted-changed-timestamp": 0,
            }
        ]
        rows = [{"id": "1", "identity_profiles": list_payload}]
        with patch.object(instance, "_fetch", return_value=rows):
            result = instance.call_api(method=None, endpoint="deals", filters=None)  # type: ignore[arg-type]
        row = result[0]
        assert row["identity_profiles_json_vid"] == 3351
        assert row["identity_profiles_json_saved-at-timestamp"] == 1613053343363
        assert row["identity_profiles_json_deleted-changed-timestamp"] == 0
        assert "identity_profiles" in row

    def test_call_api_does_not_expand_dict_value(self):
        instance = self.hubspot_instance
        rows = [{"id": "1", "form_submissions": {"a": 1, "b": 2}}]
        with patch.object(instance, "_fetch", return_value=rows):
            result = instance.call_api(method=None, endpoint="deals", filters=None)  # type: ignore[arg-type]
        row = result[0]
        assert "form_submissions_json_a" not in row
        assert "form_submissions_json_b" not in row
        assert row["form_submissions"] == {"a": 1, "b": 2}

    def test_call_api_empty_list_creates_no_expansion_columns(self):
        instance = self.hubspot_instance
        rows = [{"id": "1", "form_submissions": []}]
        with patch.object(instance, "_fetch", return_value=rows):
            result = instance.call_api(method=None, endpoint="deals", filters=None)  # type: ignore[arg-type]
        row = result[0]
        assert all(not k.startswith("form_submissions_json_") for k in row.keys())
        assert row["form_submissions"] == []

    @patch("viadot.sources.base.Source.to_df")
    def test_to_df_includes_metadata_columns(self, mock_parent_to_df):
        mock_parent_to_df.return_value = pd.DataFrame()
        df = self.hubspot_instance.to_df(data=[{"id": "x"}])
        assert "_viadot_source" in df.columns
        assert "_viadot_downloaded_at_utc" in df.columns


if __name__ == "__main__":
    unittest.main()
