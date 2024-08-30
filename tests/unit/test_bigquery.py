"""'test_bigquery.py'."""

import numpy as np
import pandas as pd
import pytest
from google.oauth2 import service_account

from viadot.exceptions import APIError, CredentialError
from viadot.sources import BigQuery
from viadot.sources.bigquery import BigQueryCredentials

variables = {
    "credentials": {
        "type": "service_account",
        "project_id": "test_project",
        "private_key_id": "some-key-id",
        "private_key": "----------",
        "client_email": "email@test.com",
        "client_id": "client_id",
        "auth_uri": "https://accounts.google.com/o/oauth2/auth",
        "token_uri": "https://oauth2.googleapis.com/token",
        "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
        "client_x509_cert_url": "https://www.googleapis.com/robot/v1/"
        + "metadata/x509/email@test.com",
    }
}


class TestBigQueryCredentials:
    """Test BigQuery Credentials Class."""

    @pytest.mark.basic
    def test_bigquery_credentials(self):
        """Test BigQuery credentials."""
        BigQueryCredentials(
            type="test_type",
            project_id="",
            private_key_id="test_private_key_id",
            private_key="test_private_key",
            client_email="test_client_email",
            client_id="test_client_id",
            auth_uri="",
            token_uri="",
            auth_provider_x509_cert_url="",
            client_x509_cert_url="",
        )


@pytest.fixture
def mock_service_account_credentials():
    """Mock BigQuery account credentials."""

    class MockCredentials:
        """Mock class."""

        def __init__(self):
            self.project_id = "mock_project_id"

    return MockCredentials()


@pytest.mark.basic
def test_missing_credentials(mocker):
    """Test raise error without BigQuery credentials."""
    mocker.patch("viadot.sources.bigquery.get_source_credentials", return_value=None)
    with pytest.raises(CredentialError):
        BigQuery(config_key="invalid_key")


@pytest.mark.functions
def test_list_datasets_query(monkeypatch, mock_service_account_credentials):
    """Test BigQuery `_list_datasets` method."""
    monkeypatch.setattr(
        service_account.Credentials,
        "from_service_account_info",
        lambda info: mock_service_account_credentials,
    )
    bigquery = BigQuery(credentials=variables["credentials"])
    expected_query = """SELECT schema_name
                FROM test_project.INFORMATION_SCHEMA.SCHEMATA"""
    query = bigquery._list_datasets()

    assert query.strip() == expected_query


@pytest.mark.functions
def test_list_tables_query(monkeypatch, mock_service_account_credentials):
    """Test BigQuery `_list_tables` method."""
    monkeypatch.setattr(
        service_account.Credentials,
        "from_service_account_info",
        lambda info: mock_service_account_credentials,
    )
    bigquery = BigQuery(credentials=variables["credentials"])
    expected_query = """SELECT table_name
                FROM test_project.test_dataset.INFORMATION_SCHEMA.TABLES"""
    query = bigquery._list_tables(dataset_name="test_dataset")

    assert query.strip() == expected_query


@pytest.mark.functions
def test_list_columns(mocker, monkeypatch, mock_service_account_credentials):
    """Test BigQuery `_list_columns` method."""
    monkeypatch.setattr(
        service_account.Credentials,
        "from_service_account_info",
        lambda info: mock_service_account_credentials,
    )
    bigquery = BigQuery(credentials=variables["credentials"])

    mock_df = pd.DataFrame({"column_name": ["col1", "col2", "col3"]})
    mocker.patch("pandas_gbq.read_gbq", return_value=mock_df)
    result = bigquery._list_columns(
        dataset_name="test_dataset",
        table_name="test_table",
    )
    expected_result = np.array(["col1", "col2", "col3"])

    np.testing.assert_array_equal(result, expected_result)


@pytest.mark.functions
def test_gbd_success(mocker, monkeypatch, mock_service_account_credentials):
    """Test BigQuery `_gbd` method."""
    monkeypatch.setattr(
        service_account.Credentials,
        "from_service_account_info",
        lambda info: mock_service_account_credentials,
    )
    bigquery = BigQuery(credentials=variables["credentials"])

    mock_df = pd.DataFrame({"col1": [1, 2], "col2": [3, 4]})
    mocker.patch("pandas_gbq.read_gbq", return_value=mock_df)
    result = bigquery._gbd("SELECT * FROM test_table")

    pd.testing.assert_frame_equal(result, mock_df)


@pytest.mark.functions
def test_gbd_failure(mocker, monkeypatch, mock_service_account_credentials):
    """Test BigQuery `_gbd` method failure."""
    monkeypatch.setattr(
        service_account.Credentials,
        "from_service_account_info",
        lambda info: mock_service_account_credentials,
    )
    bigquery = BigQuery(credentials=variables["credentials"])

    mocker.patch("pandas_gbq.read_gbq", side_effect=APIError("Error"))
    with pytest.raises(APIError):
        bigquery._gbd("SELECT * FROM test_table")


@pytest.mark.connect
def test_api_connection_list_tables(
    mocker, monkeypatch, mock_service_account_credentials
):
    """Test BigQuery `api_connection` method with predefined query."""
    monkeypatch.setattr(
        service_account.Credentials,
        "from_service_account_info",
        lambda info: mock_service_account_credentials,
    )
    bigquery = BigQuery(credentials=variables["credentials"])

    mock_df = pd.DataFrame({"table_name": ["table1", "table2"]})
    mocker.patch.object(BigQuery, "_gbd", return_value=mock_df)
    bigquery.api_connection(query="tables", dataset_name="test_dataset")

    pd.testing.assert_frame_equal(bigquery.df_data, mock_df)


@pytest.mark.connect
def test_api_connection_with_columns_and_date(
    mocker, monkeypatch, mock_service_account_credentials
):
    """Test BigQuery `api_connection` method."""
    monkeypatch.setattr(
        service_account.Credentials,
        "from_service_account_info",
        lambda info: mock_service_account_credentials,
    )
    bigquery = BigQuery(credentials=variables["credentials"])

    def mock_list_columns(self, dataset_name: str, table_name: str):
        # pylint: disable=unused-argument
        return ["column1", "column2", "test_date_column"]

    monkeypatch.setattr(BigQuery, "_list_columns", mock_list_columns)

    mock_df = pd.DataFrame(
        {"col1": [1, 2], "col2": [3, 4], "date_col": ["2023-01-01", "2023-01-02"]}
    )
    mocker.patch.object(BigQuery, "_gbd", return_value=mock_df)
    bigquery.api_connection(
        dataset_name="test_dataset",
        table_name="test_table",
        columns=["col1", "col2"],
        date_column_name="date_col",
        start_date="2023-01-01",
        end_date="2023-01-31",
    )

    pd.testing.assert_frame_equal(bigquery.df_data, mock_df)


@pytest.mark.functions
def test_to_df(monkeypatch, mock_service_account_credentials):
    """Test BigQuery `to_df` method."""
    monkeypatch.setattr(
        service_account.Credentials,
        "from_service_account_info",
        lambda info: mock_service_account_credentials,
    )
    bigquery = BigQuery(credentials=variables["credentials"])

    mock_df = pd.DataFrame({"col1": [1, 2], "col2": [3, 4]})
    bigquery.df_data = mock_df
    result = bigquery.to_df()

    pd.testing.assert_frame_equal(result, mock_df)
