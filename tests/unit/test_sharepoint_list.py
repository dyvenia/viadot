"""Tests for the SharepointList class."""

from unittest.mock import MagicMock, patch

import pandas as pd
import pytest
import requests
from sharepy.errors import AuthError

from viadot.exceptions import CredentialError
from viadot.sources import SharepointList
from viadot.sources.sharepoint import SharepointCredentials


DUMMY_CREDS = {
    "site": "tenant.sharepoint.com",  # pragma: allowlist secret`
    "username": "user@example.com",  # pragma: allowlist secret`
    "password": "password",  # pragma: allowlist secret`
}

LIST_NAME = "my_list"
LIST_SITE = "my_site"


@pytest.fixture
def mock_response():
    """Create a reusable mock response object."""
    return MagicMock()


@pytest.fixture
def mock_connection():
    """Create a reusable mock connection object."""
    conn = MagicMock()
    conn.site = "test.sharepoint.com"
    conn.get = MagicMock()
    return conn


@pytest.fixture
def sharepoint_list():
    """Create a SharepointList instance with patched connection."""
    with patch.object(SharepointList, "get_connection") as mock_get_conn:
        sp_list = SharepointList(credentials=DUMMY_CREDS)
        yield sp_list, mock_get_conn


def test_sharepoint_list_to_df_positive(
    sharepoint_list, mock_connection, mock_response
):
    """Test successful conversion of SharePoint list data to DataFrame."""
    sp_list, mock_get_conn = sharepoint_list

    # Setup mocks
    mock_get_conn.return_value = mock_connection
    mock_connection.get.return_value = mock_response

    # Configure the mock response with a small dataset
    mock_response.json.return_value = {
        "d": {
            "results": [
                {"ID": 1, "Title": "Item 1"},
                {"ID": 2, "Title": "Item 2"},
                {"ID": 3, "Title": "Item 3"},
            ]
        }
    }

    # Call the function
    df = sp_list.to_df(
        list_site=LIST_SITE,
        list_name=LIST_NAME,
        select=["ID", "Title"],
        query=None,
    )

    # Basic assertions
    assert isinstance(df, pd.DataFrame)
    assert len(df) == 3
    assert set(df.columns) >= {"ID", "Title"}

    # Verify the mock was called correctly
    expected_url = f"https://test.sharepoint.com/sites/{LIST_SITE}/_api/web/lists/GetByTitle('{LIST_NAME}')/items"
    mock_connection.get.assert_called_once()
    assert mock_connection.get.call_args[0][0] == expected_url


def test_sharepoint_list_to_df_no_items(
    sharepoint_list, mock_connection, mock_response
):
    """Test case when SharePoint list returns empty results."""
    sp_list, mock_get_conn = sharepoint_list

    # Setup mocks
    mock_get_conn.return_value = mock_connection
    mock_connection.get.return_value = mock_response

    # Configure empty response
    mock_response.json.return_value = {"d": {"results": []}}

    # Test that appropriate exception is raised
    with pytest.raises(
        ValueError, match=f"No items found in SharePoint list {LIST_NAME}"
    ):
        sp_list.to_df(
            list_site=LIST_SITE,
            list_name=LIST_NAME,
            select=["ID", "Title"],
            query=None,
        )


def test_sharepoint_list_authentication_failure():
    """Test handling of authentication failure."""
    # Use context manager to ensure proper cleanup
    with patch("sharepy.connect") as mock_connect:
        mock_connect.side_effect = AuthError("Authentication failed")

        sp_list = SharepointList(credentials=DUMMY_CREDS)

        with pytest.raises(
            CredentialError,
            match="Could not authenticate to tenant.sharepoint.com with provided credentials.",
        ):
            sp_list.get_connection()


def test_sharepoint_list_missing_credentials():
    """Test validation of missing credentials."""
    incomplete_credentials = {**DUMMY_CREDS, "site": ""}

    with pytest.raises(
        CredentialError,
        match="'site', 'username', and 'password' credentials are required.",
    ):
        SharepointCredentials(**incomplete_credentials)


def test_sharepoint_list_to_df_query_filter(
    sharepoint_list, mock_connection, mock_response
):
    """Test applying a query filter when retrieving SharePoint list data."""
    sp_list, mock_get_conn = sharepoint_list

    # Setup mocks
    mock_get_conn.return_value = mock_connection
    mock_connection.get.return_value = mock_response

    # Configure the mock response
    mock_response.json.return_value = {
        "d": {
            "results": [
                {"ID": 1, "Title": "Item 1"},
            ]
        }
    }

    # Call the function with a query
    df = sp_list.to_df(
        list_site=LIST_SITE,
        list_name=LIST_NAME,
        select=["ID", "Title"],
        query="Title eq 'Item 1'",
    )

    # Verify results
    assert isinstance(df, pd.DataFrame)
    assert len(df) == 1
    assert df["Title"].iloc[0] == "Item 1"

    # Verify proper URL construction with query parameter
    expected_url = f"https://test.sharepoint.com/sites/{LIST_SITE}/_api/web/lists/GetByTitle('{LIST_NAME}')/items"
    mock_connection.get.assert_called_once()
    assert mock_connection.get.call_args[0][0] == expected_url

    # Verify the query parameter is correctly passed in the request parameters
    params = mock_connection.get.call_args[1]["params"]
    assert "$filter" in params
    assert params["$filter"] == "Title eq 'Item 1'"
    assert "$select" in params
    assert params["$select"] == "ID,Title"


def test_rename_case_insensitive_duplicated_columns():
    """Test handling of case-insensitive duplicate column names."""
    # Create a small test DataFrame
    df = pd.DataFrame({"ID": [1, 2], "id": [3, 4], "Title": ["A", "B"]})

    # Create minimal SharepointList instance
    with patch.object(SharepointList, "get_connection"):
        sp_list = SharepointList(credentials=DUMMY_CREDS)

        # Test the method
        rename_dict = sp_list._find_and_rename_case_insensitive_duplicated_column_names(
            df
        )

        # Verify results
        assert rename_dict
        assert len(rename_dict) == 2
        assert "id" in rename_dict
        assert "ID" in rename_dict

        # Apply rename without creating another full DataFrame copy
        renamed_columns = {col: rename_dict.get(col, col) for col in df.columns}
        assert "id_1" in renamed_columns.values()
        assert "id_2" in renamed_columns.values()


def test_sharepoint_list_pagination(sharepoint_list, mock_connection):
    """Test pagination for large lists (>5000 items)."""
    sp_list, mock_get_conn = sharepoint_list

    # Create separate response objects for each pagination call
    mock_response_1 = MagicMock()
    mock_response_2 = MagicMock()
    mock_response_3 = MagicMock()

    # Create a separate connection mock
    mock_connection = MagicMock()
    mock_connection.site = "test.sharepoint.com"
    mock_connection.get = MagicMock(
        side_effect=[mock_response_1, mock_response_2, mock_response_3]
    )

    # Return our custom mock_connection
    mock_get_conn.return_value = mock_connection

    # Configure mock responses with paginated data
    mock_response_1.json.return_value = {
        "d": {
            "results": [{"ID": i, "Title": f"Item {i}"} for i in range(1, 5001)],
            "__next": "next_page_url",
        }
    }
    mock_response_2.json.return_value = {
        "d": {
            "results": [{"ID": i, "Title": f"Item {i}"} for i in range(5001, 10001)],
            "__next": "next_page_url",
        }
    }
    mock_response_3.json.return_value = {
        "d": {"results": [{"ID": i, "Title": f"Item {i}"} for i in range(10001, 15001)]}
    }

    # Call the function
    df = sp_list.to_df(
        list_site=LIST_SITE,
        list_name=LIST_NAME,
        select=["ID", "Title"],
        query=None,
    )

    # Verify correct number of API calls were made
    assert mock_connection.get.call_count == 3

    # Basic assertions
    assert isinstance(df, pd.DataFrame)
    assert len(df) == 15000
    assert df["ID"].min() == 1
    assert df["ID"].max() == 15000


def test_sharepoint_list_complex_data_types(
    sharepoint_list, mock_connection, mock_response
):
    """Test handling of complex data types in SharePoint lists."""
    sp_list, mock_get_conn = sharepoint_list

    # Setup mocks
    mock_get_conn.return_value = mock_connection
    mock_connection.get.return_value = mock_response

    # Configure the mock response with complex data types
    mock_response.json.return_value = {
        "d": {
            "results": [
                {"ID": 1, "Title": "Item 1", "ComplexField": {"SubField": "Value"}},
                {"ID": 2, "Title": "Item 2", "ComplexField": {"SubField": "Value"}},
            ]
        }
    }

    # Call the function
    df = sp_list.to_df(
        list_site=LIST_SITE,
        list_name=LIST_NAME,
        select=["ID", "Title", "ComplexField"],
        query=None,
    )

    # Basic assertions
    assert isinstance(df, pd.DataFrame)
    assert len(df) == 2
    assert set(df.columns) >= {"ID", "Title", "ComplexField"}


def test_sharepoint_list_url_construction_different_protocols(
    sharepoint_list, mock_connection, mock_response
):
    """Test URL construction with different protocols (http vs https)."""
    sp_list, mock_get_conn = sharepoint_list

    # Setup mocks
    mock_get_conn.return_value = mock_connection
    mock_connection.get.return_value = mock_response

    # Configure the mock response
    mock_response.json.return_value = {"d": {"results": [{"ID": 1, "Title": "Item 1"}]}}

    # Call the function with http protocol
    sp_list.default_protocol = "http://"
    sp_list.to_df(
        list_site=LIST_SITE,
        list_name=LIST_NAME,
        select=["ID", "Title"],
        query=None,
    )

    # Verify the mock was called correctly
    expected_url = f"http://test.sharepoint.com/sites/{LIST_SITE}/_api/web/lists/GetByTitle('{LIST_NAME}')/items"
    mock_connection.get.assert_called_once()
    assert mock_connection.get.call_args[0][0] == expected_url


def test_sharepoint_list_http_error_responses(sharepoint_list, mock_connection):
    """Test HTTP error responses with proper status code extraction."""
    sp_list, mock_get_conn = sharepoint_list

    # Create a mock response with a status code
    mock_response = MagicMock()
    mock_response.status_code = 404

    # Create an HTTP error with the response attached
    http_error = requests.exceptions.HTTPError("404 Client Error: Not Found")
    http_error.response = mock_response

    # Setup mocks
    mock_get_conn.return_value = mock_connection
    mock_connection.get.side_effect = http_error

    # Test that the appropriate exception is raised with status code information
    with pytest.raises(
        ValueError,
        match=f"HTTP error 404 when retrieving data from SharePoint list '{LIST_NAME}'",
    ):
        sp_list.to_df(
            list_site=LIST_SITE,
            list_name=LIST_NAME,
            select=["ID", "Title"],
            query=None,
        )


def test_sharepoint_list_http_error_unknown_status(sharepoint_list, mock_connection):
    """Test HTTP error responses when status code is unavailable."""
    sp_list, mock_get_conn = sharepoint_list

    # Create an HTTP error with no response
    http_error = requests.exceptions.HTTPError("Generic HTTP Error")
    # No response attribute set

    # Setup mocks
    mock_get_conn.return_value = mock_connection
    mock_connection.get.side_effect = http_error

    # Test that appropriate exception is raised with "unknown" status
    with pytest.raises(
        ValueError,
        match=f"HTTP error unknown when retrieving data from SharePoint list '{LIST_NAME}'",
    ):
        sp_list.to_df(
            list_site=LIST_SITE,
            list_name=LIST_NAME,
            select=["ID", "Title"],
            query=None,
        )


def test_sharepoint_list_timeout_handling(sharepoint_list, mock_connection):
    """Test timeout handling."""
    sp_list, mock_get_conn = sharepoint_list

    # Setup mocks
    mock_get_conn.return_value = mock_connection
    mock_connection.get.side_effect = TimeoutError("Request timed out")

    # Test that appropriate exception is raised with more specific message
    with pytest.raises(
        ValueError,
        match=f"Request to SharePoint list '{LIST_NAME}' timed out: Request timed out",
    ):
        sp_list.to_df(
            list_site=LIST_SITE,
            list_name=LIST_NAME,
            select=["ID", "Title"],
            query=None,
        )


def test_build_sharepoint_endpoint(sharepoint_list):
    """Test the _build_sharepoint_endpoint helper method."""
    sp_list, _ = sharepoint_list

    site_url = "https://test.sharepoint.com"
    endpoint = sp_list._build_sharepoint_endpoint(site_url, LIST_SITE, LIST_NAME)

    expected = f"https://test.sharepoint.com/sites/{LIST_SITE}/_api/web/lists/GetByTitle('{LIST_NAME}')/items"
    assert endpoint == expected


def test_ensure_protocol(sharepoint_list):
    """Test the _ensure_protocol helper method."""
    sp_list, _ = sharepoint_list

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


def test_get_records(sharepoint_list, mock_connection, mock_response):
    """Test the _get_records helper method."""
    sp_list, mock_get_conn = sharepoint_list

    # Setup mocks
    mock_get_conn.return_value = mock_connection
    mock_connection.get.return_value = mock_response

    mock_response.json.return_value = {
        "d": {"results": [{"ID": 1, "Title": "Item 1"}], "__next": "next_page_url"}
    }

    url = "https://test.sharepoint.com/sites/my_site/_api/web/lists/GetByTitle('my_list')/items"
    params = {"$select": "ID,Title"}

    # Call the helper method
    items, next_url = sp_list._get_records(url, params)

    # Verify results
    assert len(items) == 1
    assert items[0]["ID"] == 1
    assert items[0]["Title"] == "Item 1"
    assert next_url == "next_page_url"

    # Verify the mock was called correctly
    mock_connection.get.assert_called_once_with(url, params=params)


def test_get_records_with_next_url_object(sharepoint_list, mock_connection):
    """Test the _get_records helper with next link as an object."""
    sp_list, mock_get_conn = sharepoint_list

    # Setup mocks
    mock_response = MagicMock()
    mock_get_conn.return_value = mock_connection
    mock_connection.get.return_value = mock_response

    # Next link as an object with URI
    mock_response.json.return_value = {
        "d": {
            "results": [{"ID": 1, "Title": "Item 1"}],
            "__next": {"uri": "next_page_url_object"},
        }
    }

    url = "https://test.sharepoint.com/sites/my_site/_api/web/lists/GetByTitle('my_list')/items"

    # Call the helper method
    items, next_url = sp_list._get_records(url)

    # Verify results
    assert len(items) == 1
    assert next_url == "next_page_url_object"


def test_paginate_list_data(sharepoint_list):
    """Test the _paginate_list_data helper method."""
    sp_list, _ = sharepoint_list

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
