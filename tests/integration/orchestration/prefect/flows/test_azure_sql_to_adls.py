from unittest.mock import patch

import pandas as pd
import pytest

from viadot.orchestration.prefect.flows import azure_sql_to_adls
from viadot.sources.azure_data_lake import AzureDataLake


@pytest.fixture
def query():
    return "SELECT * FROM your_table_name"


@pytest.fixture
def TEST_FILE_PATH():
    return "test_file_path"


@pytest.fixture
def adls_credentials_secret():
    return "mock_adls_credentials_secret"


@pytest.fixture
def azure_sql_credentials_secret():
    return "mock_azure_sql_credentials_secret"


def test_azure_sql_to_adls(
    query,
    TEST_FILE_PATH,
    adls_credentials_secret,
    azure_sql_credentials_secret,
):
    lake = AzureDataLake(config_key="adls_test")

    # Ensure the file does not exist before the test
    assert not lake.exists(TEST_FILE_PATH)

    with (
        patch(
            "viadot.orchestration.prefect.tasks.azure_sql_to_df"
        ) as mock_azure_sql_to_df,
        patch("viadot.orchestration.prefect.tasks.df_to_adls") as mock_df_to_adls,
    ):
        mock_df = pd.DataFrame({"column1": [1, 2], "column2": [3, 4]})
        mock_azure_sql_to_df.return_value = mock_df

        # Call the flow
        azure_sql_to_adls(
            query=query,
            credentials_secret=azure_sql_credentials_secret,
            validate_df_dict=None,
            convert_bytes=False,
            remove_special_characters=None,
            columns_to_clean=None,
            adls_config_key="adls_test",
            adls_azure_key_vault_secret=adls_credentials_secret,
            adls_path=TEST_FILE_PATH,
            adls_path_overwrite=True,
        )

        # Assert that the azure_sql_to_df task was called with the correct arguments
        mock_azure_sql_to_df.assert_called_once_with(
            query=query,
            credentials_secret=azure_sql_credentials_secret,
            sep=",",
            file_path=TEST_FILE_PATH,
            if_exists="replace",
            validate_df_dict=None,
            convert_bytes=False,
            remove_special_characters=None,
            columns_to_clean=None,
        )

        # Assert that df_to_adls was called with the correct arguments
        mock_df_to_adls.assert_called_once_with(
            df=mock_df,
            path=TEST_FILE_PATH,
            credentials_secret=adls_credentials_secret,
            config_key="adls_test",
            overwrite=True,
        )

    assert lake.exists(TEST_FILE_PATH)

    lake.rm(TEST_FILE_PATH)
