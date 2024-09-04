from datetime import datetime
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest
from viadot.orchestration.prefect.exceptions import MissingSourceCredentialsError
from viadot.orchestration.prefect.tasks import customer_gauge_to_df


@pytest.fixture
def mock_credentials():
    return {"client_id": "test_client_id", "client_secret": "test_client_secret"}


@patch("customer_gauge_to_df.get_credentials")
@patch("customer_gauge_to_df.CustomerGauge")
def test_customer_gauge_to_df_success(
    mock_customer_gauge, mock_get_credentials, mock_credentials
):
    # Mock the credentials and the CustomerGauge class
    mock_get_credentials.return_value = mock_credentials
    mock_instance = MagicMock()
    mock_customer_gauge.return_value = mock_instance

    # Mock the to_df method to return a sample DataFrame
    expected_df = pd.DataFrame({"column1": [1, 2, 3], "column2": ["a", "b", "c"]})
    mock_instance.to_df.return_value = expected_df

    # Call the function
    result_df = customer_gauge_to_df(
        endpoint="responses",
        start_date=datetime(2023, 1, 1),
        end_date=datetime(2023, 12, 31),
        credentials=mock_credentials,
    )

    # Assertions
    mock_get_credentials.assert_called_once()
    mock_customer_gauge.assert_called_once_with(
        endpoint="responses",
        endpoint_url=None,
        total_load=True,
        cursor=None,
        pagesize=1000,
        date_field=None,
        start_date=datetime(2023, 1, 1),
        end_date=datetime(2023, 12, 31),
        unpack_by_field_reference_cols=None,
        unpack_by_nested_dict_transformer=None,
        credentials=mock_credentials,
        anonymize=False,
        columns_to_anonymize=None,
        anonymize_method="mask",
        anonymize_value="***",
        date_column=None,
        days=None,
        validate_df_dict=None,
        timeout=3600,
    )
    assert result_df.equals(expected_df)


@patch("customer_gauge_to_df.get_credentials")
def test_customer_gauge_to_df_missing_credentials(mock_get_credentials):
    # Simulate missing credentials
    mock_get_credentials.return_value = None

    # Call the function and expect an exception
    with pytest.raises(MissingSourceCredentialsError):
        customer_gauge_to_df(
            endpoint="responses",
            start_date=datetime(2023, 1, 1),
            end_date=datetime(2023, 12, 31),
        )
