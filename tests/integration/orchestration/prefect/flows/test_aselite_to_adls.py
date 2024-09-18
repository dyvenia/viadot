from src.viadot.orchestration.prefect.flows import aselite_to_adls
from src.viadot.sources import AzureDataLake
from unittest.mock import patch

def test_aselite_to_adls(
    query,
    TEST_FILE_PATH,
    adls_credentials_secret,
    aselite_credentials_secret,
):
    # Mock Azure Data Lake object
    lake = AzureDataLake(config_key="adls_test")

    # Ensure the file does not exist before the test
    assert not lake.exists(TEST_FILE_PATH)

    # Mock the `aselite_to_df` and `df_to_adls` tasks
    with patch("my_module.aselite_to_df") as mock_aselite_to_df, \
         patch("my_module.df_to_adls") as mock_df_to_adls:

        # Prepare mock DataFrame
        mock_df = mock_aselite_to_df.return_value

        # Call the flow
        aselite_to_adls(
            query=query,
            credentials_secret=aselite_credentials_secret,
            sep=",",
            file_path=TEST_FILE_PATH,
            if_exists="replace",
            validate_df_dict=None,
            convert_bytes=False,
            remove_special_characters=None,
            columns_to_clean=None,
            adls_config_key="adls_test",
            adls_azure_key_vault_secret=adls_credentials_secret,
            adls_path=TEST_FILE_PATH,
            adls_path_overwrite=True,
        )

        # Assert that the aselite_to_df task was called with the correct arguments
        mock_aselite_to_df.assert_called_once_with(
            query=query,
            credentials_secret=aselite_credentials_secret,
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

    # Ensure the file now exists in the Data Lake
    assert lake.exists(TEST_FILE_PATH)

    # Clean up after the test by removing the test file from ADLS
    lake.rm(TEST_FILE_PATH)