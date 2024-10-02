import pytest

from viadot.sources.azure_sql import AzureSQL
from viadot.sources.sql_server import SQLServerCredentials


@pytest.fixture
def azure_sql_credentials():
    return SQLServerCredentials(
        user="test_user",
        password="test_password",  # pragma: allowlist secret # noqa: S106
        server="localhost",
        db_name="test_db",
        driver="ODBC Driver 17 for SQL Server",
    )


@pytest.fixture
def azure_sql(azure_sql_credentials: SQLServerCredentials, mocker):
    mocker.patch("viadot.sources.base.SQL.con", return_value=True)

    return AzureSQL(
        credentials={
            "user": azure_sql_credentials.user,
            "password": azure_sql_credentials.password,
            "server": azure_sql_credentials.server,
            "db_name": azure_sql_credentials.db_name,
            "data_source": "test_data_source",
        }
    )


def test_azure_sql_initialization(azure_sql):
    """Test that the AzureSQL object is initialized with the correct credentials."""
    assert azure_sql.credentials["server"] == "localhost"
    assert azure_sql.credentials["user"] == "test_user"
    assert (
        azure_sql.credentials["password"]
        == "test_password"  # pragma: allowlist secret  # noqa: S105
    )
    assert azure_sql.credentials["db_name"] == "test_db"


def test_create_external_database(azure_sql, mocker):
    """Test the create_external_database function."""
    mock_run = mocker.patch("viadot.sources.base.SQL.run", return_value=True)

    # Test parameters
    external_database_name = "test_external_db"
    storage_account_name = "test_storage_account"
    container_name = "test_container"
    sas_token = "test_sas_token"  # noqa: S105
    master_key_password = (
        "test_master_key_password"  # pragma: allowlist secret # noqa: S105
    )
    credential_name = "custom_credential_name"

    azure_sql.create_external_database(
        external_database_name=external_database_name,
        storage_account_name=storage_account_name,
        container_name=container_name,
        sas_token=sas_token,
        master_key_password=master_key_password,
        credential_name=credential_name,
    )

    # Expected SQL commands with custom credential name
    expected_master_key_sql = (
        f"CREATE MASTER KEY ENCRYPTION BY PASSWORD = {master_key_password}"
    )

    mock_run.assert_any_call(expected_master_key_sql)
    assert mock_run.call_count == 3  # Ensure all 3 SQL commands were executed
