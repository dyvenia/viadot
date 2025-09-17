"""Tests for the SharepointList class."""

from unittest.mock import patch

import pandas as pd
import pytest

from viadot.exceptions import CredentialError
from viadot.sources import SharepointList
from viadot.sources.sharepoint import SharepointCredentials


DUMMY_CREDS = {
    "site": "tenant.sharepoint.com",  # pragma: allowlist secret
    "client_id": "client",  # pragma: allowlist secret
    "client_secret": "secret",  # pragma: allowlist secret
    "tenant_id": "tenant",  # pragma: allowlist secret
}

LIST_NAME = "my_list"
LIST_SITE = "my_site"


@pytest.fixture
def sharepoint_list():
    """Create a SharepointList instance."""
    return SharepointList(credentials=DUMMY_CREDS)


def test_sharepoint_list_to_df_positive(sharepoint_list):
    """Test successful conversion of SharePoint list data to DataFrame."""
    sp_list = sharepoint_list

    items = [
        {"ID": 1, "Title": "Item 1"},
        {"ID": 2, "Title": "Item 2"},
        {"ID": 3, "Title": "Item 3"},
    ]

    with patch.object(SharepointList, "_paginate_list_data", return_value=items):
        df = sp_list.to_df(
            list_site=LIST_SITE,
            list_name=LIST_NAME,
            select=["ID", "Title"],
            query=None,
        )

    assert isinstance(df, pd.DataFrame)
    assert len(df) == 3
    assert set(df.columns) >= {"ID", "Title"}


def test_sharepoint_list_to_df_no_items(sharepoint_list):
    """Test case when SharePoint list returns empty results."""
    sp_list = sharepoint_list

    with (
        patch.object(
            SharepointList,
            "_paginate_list_data",
            side_effect=ValueError(f"No items found in SharePoint list {LIST_NAME}"),
        ),
        pytest.raises(
            ValueError, match=f"No items found in SharePoint list {LIST_NAME}"
        ),
    ):
        sp_list.to_df(
            list_site=LIST_SITE,
            list_name=LIST_NAME,
            select=["ID", "Title"],
            query=None,
        )


def test_sharepoint_list_authentication_failure():
    """Test handling of authentication failure using Graph client."""
    sp_list = SharepointList(credentials=DUMMY_CREDS)

    with (
        patch(
            "viadot.sources.sharepoint.GraphClient",
            side_effect=Exception("Authentication failed"),
        ),
        pytest.raises(
            CredentialError,
            match=(
                "Could not authenticate to tenant.sharepoint.com "
                "with provided credentials."
            ),
        ),
    ):
        sp_list.get_client()


def test_sharepoint_list_missing_credentials():
    """Test validation of missing credentials."""
    incomplete_credentials = {
        "site": "",  # pragma: allowlist secret
        "client_id": "client",  # pragma: allowlist secret
        "client_secret": "secret",  # pragma: allowlist secret
        "tenant_id": "tenant",  # pragma: allowlist secret
    }

    with pytest.raises(
        CredentialError,
        match=(
            "'site', 'client_id', 'client_secret' and "
            "'tenant_id' credentials are required."
        ),
    ):
        SharepointCredentials(**incomplete_credentials)


def test_sharepoint_list_to_df_query_filter(sharepoint_list):
    """Test applying a query filter when retrieving SharePoint list data."""
    sp_list = sharepoint_list

    with patch.object(
        SharepointList,
        "_paginate_list_data",
        return_value=[{"ID": 1, "Title": "Item 1"}],
    ) as mock_paginate:
        df = sp_list.to_df(
            list_site=LIST_SITE,
            list_name=LIST_NAME,
            select=["ID", "Title"],
            query="Title eq 'Item 1'",
        )

    assert isinstance(df, pd.DataFrame)
    assert len(df) == 1
    assert df["Title"].iloc[0] == "Item 1"

    # Verify endpoint and params passed to pagination
    site_url = sp_list._ensure_protocol(sp_list.credentials.get("site"))
    expected_url = sp_list._build_sharepoint_endpoint(site_url, LIST_SITE, LIST_NAME)
    args, kwargs = mock_paginate.call_args
    assert args[0] == expected_url
    assert args[1]["$filter"] == "Title eq 'Item 1'"
    assert args[1]["$select"] == "ID,Title"


def test_rename_case_insensitive_duplicated_columns():
    """Test handling of case-insensitive duplicate column names."""
    # Create a small test DataFrame
    df = pd.DataFrame({"ID": [1, 2], "id": [3, 4], "Title": ["A", "B"]})

    sp_list = SharepointList(credentials=DUMMY_CREDS)

    # Test the method
    rename_dict = sp_list._find_and_rename_case_insensitive_duplicated_column_names(df)

    # Verify results
    assert rename_dict
    assert len(rename_dict) == 2
    assert "id" in rename_dict
    assert "ID" in rename_dict

    # Apply rename without creating another full DataFrame copy
    renamed_columns = {col: rename_dict.get(col, col) for col in df.columns}
    assert "id_1" in renamed_columns.values()
    assert "id_2" in renamed_columns.values()


def test_sharepoint_list_pagination(sharepoint_list):
    """Test pagination for large lists (>5000 items)."""
    sp_list = sharepoint_list

    # Mock _get_records to simulate pagination
    with patch.object(SharepointList, "_get_records") as mock_fetch:
        mock_fetch.side_effect = [
            (
                [{"ID": i, "Title": f"Item {i}"} for i in range(1, 5001)],
                "next_page_url",
            ),
            (
                [{"ID": i, "Title": f"Item {i}"} for i in range(5001, 10001)],
                "next_page_url",
            ),
            ([{"ID": i, "Title": f"Item {i}"} for i in range(10001, 15001)], None),
        ]

        df = sp_list.to_df(
            list_site=LIST_SITE,
            list_name=LIST_NAME,
            select=["ID", "Title"],
            query=None,
        )

    assert isinstance(df, pd.DataFrame)
    assert len(df) == 15000
    assert df["ID"].min() == 1
    assert df["ID"].max() == 15000


def test_sharepoint_list_complex_data_types(sharepoint_list):
    """Test handling of complex data types in SharePoint lists."""
    sp_list = sharepoint_list

    complex_items = [
        {"ID": 1, "Title": "Item 1", "ComplexField": {"SubField": "Value"}},
        {"ID": 2, "Title": "Item 2", "ComplexField": {"SubField": "Value"}},
    ]

    with patch.object(
        SharepointList, "_paginate_list_data", return_value=complex_items
    ):
        df = sp_list.to_df(
            list_site=LIST_SITE,
            list_name=LIST_NAME,
            select=["ID", "Title", "ComplexField"],
            query=None,
        )

    assert isinstance(df, pd.DataFrame)
    assert len(df) == 2
    assert set(df.columns) >= {"ID", "Title", "ComplexField"}


def test_sharepoint_list_url_construction_different_protocols(sharepoint_list):
    """Test URL construction with different protocols (http vs https)."""
    sp_list = sharepoint_list

    with patch.object(
        SharepointList,
        "_paginate_list_data",
        return_value=[{"ID": 1, "Title": "Item 1"}],
    ) as mock_paginate:
        sp_list.default_protocol = "http://"
        sp_list.to_df(
            list_site=LIST_SITE,
            list_name=LIST_NAME,
            select=["ID", "Title"],
            query=None,
        )

    expected_url = (
        f"http://{sp_list.credentials.get('site')}/sites/"
        f"{LIST_SITE}/_api/web/lists/GetByTitle('{LIST_NAME}')/items"
    )
    args, _ = mock_paginate.call_args
    assert args[0] == expected_url


def test_sharepoint_list_error_wrapped_with_list_name(sharepoint_list):
    """Errors from _get_records should be wrapped to include list name."""
    sp_list = sharepoint_list

    with (
        patch.object(
            SharepointList,
            "_get_records",
            side_effect=ValueError(
                "Failed to retrieve data from SharePoint list: boom"
            ),
        ),
        pytest.raises(
            ValueError,
            match=("Failed to retrieve data from SharePoint list 'my_list': boom"),
        ),
    ):
        sp_list._paginate_list_data("initial_url", None, LIST_NAME)


def test_sharepoint_list_timeout_handling(sharepoint_list):
    """Test timeout handling message wrapping."""
    sp_list = sharepoint_list

    with (
        patch.object(
            SharepointList,
            "_get_records",
            side_effect=ValueError(
                "Request to SharePoint list timed out: Request timed out"
            ),
        ),
        pytest.raises(
            ValueError,
            match=("Request to SharePoint list 'my_list' timed out: Request timed out"),
        ),
    ):
        sp_list._paginate_list_data("initial_url", None, LIST_NAME)


def test_build_sharepoint_endpoint(sharepoint_list):
    """Test the _build_sharepoint_endpoint helper method."""
    sp_list = sharepoint_list

    site_url = "https://test.sharepoint.com"
    endpoint = sp_list._build_sharepoint_endpoint(site_url, LIST_SITE, LIST_NAME)

    expected = (
        f"https://test.sharepoint.com/sites/{LIST_SITE}/_api/web/lists/"
        f"GetByTitle('{LIST_NAME}')/items"
    )
    assert endpoint == expected


def test_ensure_protocol(sharepoint_list):
    """Test the _ensure_protocol helper method."""
    sp_list = sharepoint_list

    # Default protocol should be "https://"
    site_without_protocol = "test.sharepoint.com"
    site_with_protocol = sp_list._ensure_protocol(site_without_protocol)
    assert site_with_protocol == "https://test.sharepoint.com"

    # Should not modify URLs that already have a protocol
    site_already_with_protocol = "https://test.sharepoint.com"
    result = sp_list._ensure_protocol(site_already_with_protocol)
    assert result == site_already_with_protocol

    # Should handle different protocols
    sp_list.default_protocol = "http://"
    result = sp_list._ensure_protocol(site_without_protocol)
    assert result == "http://test.sharepoint.com"


def test_paginate_list_data(sharepoint_list):
    """Test the _paginate_list_data helper method."""
    sp_list = sharepoint_list

    # We'll mock __get_records directly to test pagination logic
    with patch.object(SharepointList, "_get_records") as mock_fetch:
        # Set up the mock to return different results for each call
        mock_fetch.side_effect = [
            # First call returns 2 items and a next link
            ([{"ID": 1}, {"ID": 2}], "next_page_url"),
            # Second call returns 3 items and no next link
            ([{"ID": 3}, {"ID": 4}, {"ID": 5}], None),
        ]
        # Call the paginate method
        results = sp_list._paginate_list_data(
            "initial_url", {"param": "value"}, LIST_NAME
        )

        # Verify results
        assert len(results) == 5
        assert [item["ID"] for item in results] == [1, 2, 3, 4, 5]

        # Verify mock calls
        assert mock_fetch.call_count == 2
        mock_fetch.assert_any_call("initial_url", {"param": "value"})
        mock_fetch.assert_any_call("next_page_url", None)
