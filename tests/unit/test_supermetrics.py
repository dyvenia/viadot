import pytest

from viadot.sources import Supermetrics
from viadot.sources.supermetrics import SupermetricsCredentials


@pytest.fixture
def supermetrics_credentials():
    return SupermetricsCredentials(
        user="test_user",
        api_key="test_key",  # pragma: allowlist secret
    )


@pytest.fixture
def mock_get_source_credentials(
    mocker,
    supermetrics_credentials: SupermetricsCredentials,
):
    return mocker.patch(
        "viadot.config.get_source_credentials",
        return_value={
            "user": supermetrics_credentials.user,
            "api_key": supermetrics_credentials.api_key,
        },
    )


@pytest.fixture
def supermetrics(supermetrics_credentials: SupermetricsCredentials):
    return Supermetrics(
        credentials={
            "user": supermetrics_credentials.user,
            "api_key": supermetrics_credentials.api_key,
        },
        query_params={"ds_id": "GA", "query": "test_query"},
    )


def test_to_json(mocker, supermetrics: Supermetrics):
    # Mock the handle_api_response function to simulate an API response
    mock_handle_api_response = mocker.patch(
        "viadot.sources.supermetrics.handle_api_response",
    )
    mock_response = {
        "data": [["value1", "value2"]],
        "meta": {"query": {"fields": [{"field_name": "col1"}, {"field_name": "col2"}]}},
    }
    # Set the mock to return the mock response object
    mock_handle_api_response.return_value.json.return_value = mock_response

    # Call the method under test
    response = supermetrics.to_json()

    # Assert that the response is as expected
    assert response == {
        "data": [["value1", "value2"]],
        "meta": {"query": {"fields": [{"field_name": "col1"}, {"field_name": "col2"}]}},
    }


def test_to_df_with_data(supermetrics: Supermetrics, mocker):
    # Mock the API response with some data
    mock_response = {
        "meta": {
            "query": {
                "ds_id": "GA",  # Data source ID, e.g., Google Analytics
                "fields": [
                    {
                        "field_name": "date",
                        "field_type": "DIMENSION",
                        "field_split": "row",
                    },
                    {
                        "field_name": "sessions",
                        "field_type": "METRIC",
                        "field_split": "row",
                    },
                ],
                "other_query_metadata": "...",
            },
            "status": "success",  # Status of the query
            "execution_time": "0.456",  # Time taken to execute the query
        },
        "data": [
            ["2023-01-01", 100],  # Example data rows
            ["2023-01-02", 150],
            ["2023-01-03", 120],
        ],
        "paging": {
            "current_page": 1,  # Current page number if pagination is used
            "total_pages": 1,
            "total_results": 3,
        },
    }

    mock_method = mocker.patch("viadot.sources.supermetrics.Supermetrics.to_json")
    mock_method.return_value = mock_response
    mock_method = mocker.patch(
        "viadot.sources.supermetrics.Supermetrics._get_col_names",
    )
    mock_method.return_value = ["date", "sessions"]
    df = supermetrics.to_df()

    assert not df.empty
    assert list(df.columns) == [
        "date",
        "sessions",
        "_viadot_source",
        "_viadot_downloaded_at_utc",
    ]


def test_get_col_names_google_analytics_pivoted(supermetrics: Supermetrics):
    mock_response = {
        "meta": {
            "query": {
                "fields": [
                    {"field_name": "ga:date", "field_split": "column"},
                    {"field_name": "ga:sessions", "field_split": "row"},
                ],
            },
        },
        "data": [{"ga:date": "2023-01-01", "ga:sessions": 100}],
    }
    columns = supermetrics._get_col_names_google_analytics(mock_response)
    assert columns == {"ga:date": "2023-01-01", "ga:sessions": 100}


def test_get_col_names_google_analytics_non_pivoted(supermetrics: Supermetrics):
    mock_response = {
        "meta": {
            "query": {
                "fields": [
                    {"field_name": "ga:date", "field_split": "row"},
                    {"field_name": "ga:sessions", "field_split": "row"},
                ],
            },
        },
        "data": [{"ga:date": "2023-01-01", "ga:sessions": 100}],
    }
    columns = supermetrics._get_col_names_google_analytics(mock_response)
    assert columns == ["ga:date", "ga:sessions"]


def test_to_df_metadata_columns(mocker, supermetrics: Supermetrics):
    # Mock the API response with some data
    mock_response = {
        "data": [["2023-01-01", 100]],
        "meta": {
            "query": {"fields": [{"field_name": "date"}, {"field_name": "sessions"}]},
        },
    }

    mocker.patch(
        "viadot.sources.supermetrics.Supermetrics.to_json",
        return_value=mock_response,
    )
    mocker.patch(
        "viadot.sources.supermetrics.Supermetrics._get_col_names",
        return_value=["date", "sessions"],
    )

    df = supermetrics.to_df()

    assert "_viadot_source" in df.columns
    assert "_viadot_downloaded_at_utc" in df.columns


def test_get_col_names_ga(mocker, supermetrics: Supermetrics):
    mocker.patch(
        "viadot.sources.supermetrics.Supermetrics.to_json",
        return_value={
            "meta": {
                "query": {
                    "fields": [
                        {"field_name": "ga:date", "field_split": "column"},
                        {"field_name": "ga:sessions", "field_split": "row"},
                    ],
                },
            },
            "data": [{"ga:date": "2023-01-01", "ga:sessions": 100}],
        },
    )
    columns = supermetrics._get_col_names()
    assert columns == {"ga:date": "2023-01-01", "ga:sessions": 100}
