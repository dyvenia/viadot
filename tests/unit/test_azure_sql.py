import unittest
from unittest.mock import patch

from viadot.sources.azure_sql import AzureSQL


class TestAzureSQL(unittest.TestCase):
    @patch("src.viadot.sources.SQLServer.run")
    def test_bulk_insert_default(self, mock_run):
        """Test the `bulk_insert` function with default parameters."""
        azure_sql = AzureSQL()

        result = azure_sql.bulk_insert(table="test_table", source_path="/path/to/file")

        mock_run.assert_called_once()

        assert result

    @patch("src.viadot.sources.SQLServer.run")
    def test_bulk_insert_with_replace(self, mock_run):
        """Test the `bulk_insert` function with the `replace` option."""
        azure_sql = AzureSQL()

        azure_sql.bulk_insert(
            table="test_table", source_path="/path/to/file", if_exists="replace"
        )

        delete_sql = "DELETE FROM dbo.test_table"
        bulk_insert_sql = (
            "BULK INSERT dbo.test_table FROM '/path/to/file' WITH ("
            "CHECK_CONSTRAINTS, DATA_SOURCE='None', DATAFILETYPE='char', "
            "FIELDTERMINATOR='\\t', ROWTERMINATOR='0x0a', FIRSTROW=2, "
            "KEEPIDENTITY, TABLOCK, CODEPAGE='65001');"
        )

        mock_run.assert_any_call(delete_sql)
        mock_run.assert_any_call(bulk_insert_sql)

    @patch("src.viadot.sources.SQLServer.run")
    def test_create_external_database(self, mock_run):
        """Test the `create_external_database` function."""
        azure_sql = AzureSQL()

        external_database_name = "external_db"
        storage_account_name = "mystorageaccount"
        container_name = "mycontainer"
        sas_token = "sastoken123"  # noqa: S105
        master_key_password = "masterpapssword"  # pragma: allowlist secret

        azure_sql.create_external_database(
            external_database_name=external_database_name,
            storage_account_name=storage_account_name,
            container_name=container_name,
            sas_token=sas_token,
            master_key_password=master_key_password,
        )

        mock_run.assert_any_call(
            "CREATE MASTER KEY ENCRYPTION BY PASSWORD = masterpassword"
        )

        credential_sql = (
            "CREATE DATABASE SCOPED CREDENTIAL external_db_credential "
            "WITH IDENTITY = 'SHARED ACCESS SIGNATURE' SECRET = 'sastoken123';"  # pragma: allowlist secret
        )
        mock_run.assert_any_call(credential_sql)

        external_db_sql = (
            "CREATE EXTERNAL DATA SOURCE external_db WITH ("
            "LOCATION = 'https://mystorageaccount.blob.core.windows.net/mycontainer', "
            "CREDENTIAL = external_db_credential);"
        )
        mock_run.assert_any_call(external_db_sql)
