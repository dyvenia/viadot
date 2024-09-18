import pytest
from unittest.mock import patch, MagicMock
import pandas as pd
from src.viadot.orchestration.prefect.tasks import aselite_to_df  


@pytest.fixture
def sample_dataframe():
    """Fixture for a sample dataframe."""
    data = {"column1": [1, 2], "column2": [3, 4]}
    return pd.DataFrame(data)


@patch("my_module.get_credentials")
@patch("my_module.AzureSQL")
@patch("my_module.df_converts_bytes_to_int")
@patch("my_module.df_clean_column")
@patch("my_module.df_to_csv")
def test_aselite_to_df(
    mock_df_to_csv,
    mock_df_clean_column,
    mock_df_converts_bytes_to_int,
    mock_azure_sql,
    mock_get_credentials,
    sample_dataframe
):
    # Mocking credentials and AzureSQL to return expected values
    mock_get_credentials.return_value = {"user": "test_user", "password": "test_pass"}
    mock_azure_sql_instance = mock_azure_sql.return_value
    mock_azure_sql_instance.to_df.return_value = sample_dataframe

    # Test case when convert_bytes is True
    mock_df_converts_bytes_to_int.return_value = sample_dataframe

    # Call the function
    aselite_to_df_result = aselite_to_df(
        query="SELECT * FROM test_table",
        credentials_secret="test_secret",
        sep=",",
        file_path="test.csv",
        if_exists="replace",
        validate_df_dict=None,
        convert_bytes=True,
        remove_special_characters=None,
        columns_to_clean=None,
    )

    # Assertions
    mock_get_credentials.assert_called_once_with("test_secret")
    mock_azure_sql_instance.to_df.assert_called_once_with(
        query="SELECT * FROM test_table")
    mock_df_converts_bytes_to_int.assert_called_once_with(df=sample_dataframe)
    mock_df_to_csv.assert_called_once_with(
        df=sample_dataframe, path="test.csv", sep=",", if_exists="replace"
    )

    # Test case when remove_special_characters is True
    mock_df_clean_column.return_value = sample_dataframe

    aselite_to_df(
        query="SELECT * FROM test_table",
        credentials_secret="test_secret",
        sep=",",
        file_path="test.csv",
        if_exists="replace",
        validate_df_dict=None,
        convert_bytes=False,
        remove_special_characters=True,
        columns_to_clean=["column1"],
    )

    mock_df_clean_column.assert_called_once_with(
        df=sample_dataframe, columns_to_clean=["column1"])

    # Test case for missing credentials_secret
    with pytest.raises(ValueError, 
                       match="`credentials_secret` has to be specified and not empty."):
        aselite_to_df(
            query="SELECT * FROM test_table",
            credentials_secret=None,
        )
