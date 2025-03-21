"""Tests for the sharepoint_list_to_df task."""

import pandas as pd
import pytest

from viadot.orchestration.prefect.tasks import sharepoint_list_to_df
from viadot.sources.sharepoint import SharepointList


DUMMY_CREDS = {
    "site": "test@domain.com",  # pragma: allowlist secret
    "username": "test2",  # pragma: allowlist secret
    "password": "test",  # pragma: allowlist secret
}


class SharepointListMock(SharepointList):
    """Custom mock class for SharepointList."""

    def __init__(self, *args, **kwargs):  # noqa: D107
        super().__init__(*args, **kwargs)
        self.base_data = [
            {"ID": 1, "Title": "Item 1", "Description": "Desc 1"},
            {"ID": 2, "Title": "Item 2", "Description": "Desc 2"},
            {"ID": 3, "Title": "Item 3", "Description": "Desc 3"},
        ]

    def get_items(self, *, query=None, select=None):
        """Mock implementation of get_items method with query and select support."""
        filtered_data = self.apply_query(self.base_data, query)
        return self.apply_select(filtered_data, select)

    def apply_query(self, data, query):
        """Apply the query filter to the data."""
        if not query:
            return data
        # Simplified query handling (e.g., "Title eq 'Item 1'")
        if " eq " in query:
            column, value = query.split(" eq ")
            return [item for item in data if item.get(column, "") == value]
        return data

    def apply_select(self, data, select):
        """Select only the specified columns from the data."""
        if not select:
            return data
        selected_data = []
        for item in data:
            selected_item = {key: item[key] for key in select if key in item}
            selected_data.append(selected_item)
        return selected_data


@pytest.fixture
def sharepoint_list_mock():
    """Fixture to provide a mock for SharepointList."""
    return SharepointListMock(credentials={"dummy": "credentials"})


@pytest.mark.unit
def test_sharepoint_list_to_df_positive(sharepoint_list_mock):
    """Test the positive case where the function runs successfully."""
    # Call the function
    df = sharepoint_list_to_df(
        list_site="site",
        list_name="list",
        select=["ID", "Title"],
        query=None,
        credentials_secret=DUMMY_CREDS,
        config_key="key",
    )

    # Assert the result
    assert isinstance(df, pd.DataFrame)
    assert len(df) == 3
    assert "ID" in df.columns
    assert "Title" in df.columns
    assert "Description" not in df.columns

    # Verify the mock was called correctly
    sharepoint_list_mock.get_items.assert_called_once_with(
        query=None,
        select=["ID", "Title"],
    )


@pytest.mark.unit
def test_sharepoint_list_to_df_with_query(sharepoint_list_mock):
    """Test the function with a query parameter."""
    # Call the function with a query
    df = sharepoint_list_to_df(
        list_site="site",
        list_name="list",
        select=["ID", "Title"],
        query="Title eq 'Item 1'",
        credentials_secret=DUMMY_CREDS,
        config_key="key",
    )

    # Assert the result
    assert isinstance(df, pd.DataFrame)
    assert len(df) == 1
    assert df["Title"].iloc[0] == "Item 1"

    # Verify the mock was called correctly
    sharepoint_list_mock.get_items.assert_called_once_with(
        query="Title eq 'Item 1'",
        select=["ID", "Title"],
    )


@pytest.mark.unit
def test_sharepoint_list_to_df_with_select(sharepoint_list_mock):
    """Test the function with the select parameter."""
    # Call the function with select
    df = sharepoint_list_to_df(
        list_site="site",
        list_name="list",
        select=["ID", "Description"],
        query=None,
        credentials_secret=DUMMY_CREDS,
        config_key="key",
    )

    # Assert the result
    assert isinstance(df, pd.DataFrame)
    assert len(df) == 3
    assert "ID" in df.columns
    assert "Description" in df.columns
    assert "Title" not in df.columns

    # Verify the mock was called correctly
    sharepoint_list_mock.get_items.assert_called_once_with(
        query=None,
        select=["ID", "Description"],
    )


@pytest.mark.unit
def test_sharepoint_list_to_df_with_query_and_select(sharepoint_list_mock):
    """Test the function with both query and select parameters."""
    # Call the function with query and select
    df = sharepoint_list_to_df(
        list_site="site",
        list_name="list",
        select=["ID", "Title"],
        query="Title eq 'Item 2'",
        credentials_secret=DUMMY_CREDS,
        config_key="key",
    )

    # Assert the result
    assert isinstance(df, pd.DataFrame)
    assert len(df) == 1
    assert df["Title"].iloc[0] == "Item 2"

    # Verify the mock was called correctly
    sharepoint_list_mock.get_items.assert_called_once_with(
        query="Title eq 'Item 2'",
        select=["ID", "Title"],
    )


@pytest.mark.unit
def test_sharepoint_list_to_df_empty_data(sharepoint_list_mock):
    """Test the case where the SharePoint list returns no items."""
    # Setup mock to return an empty list
    sharepoint_list_mock.base_data = []

    # Call the function
    df = sharepoint_list_to_df(
        list_site="site",
        list_name="list",
        select=["ID", "Title"],
        query=None,
        credentials_secret=DUMMY_CREDS,
        config_key="key",
    )

    # Assert the result
    assert isinstance(df, pd.DataFrame)
    assert len(df) == 0

    # Verify the mock was called correctly
    sharepoint_list_mock.get_items.assert_called_once_with(
        query=None,
        select=["ID", "Title"],
    )


@pytest.mark.unit
def test_sharepoint_list_to_df_negative(sharepoint_list_mock):
    """Test the negative case where the function raises an error."""
    # Setup mock to raise an error
    sharepoint_list_mock.get_items.side_effect = ValueError("SharePoint error")

    # Call the function and assert it raises the error
    with pytest.raises(ValueError, match="SharePoint error"):
        sharepoint_list_to_df(
            list_site="site",
            list_name="list",
            select=["ID", "Title"],
            query=None,
            credentials_secret=DUMMY_CREDS,
            config_key="key",
        )

    # Verify the mock was called correctly
    sharepoint_list_mock.get_items.assert_called_once_with(
        query=None,
        select=["ID", "Title"],
    )
