from unittest.mock import Mock, patch

import pandas as pd
import pytest

from viadot.sources.business_core import BusinessCore


@pytest.fixture(scope="module")
def business_core():
    return BusinessCore(
        url="https://api.businesscore.ae/api/GetCustomerData",
        filters={
            "BucketCount": 10,
            "BucketNo": 1,
            "FromDate": None,
            "ToDate": None,
        },
        credentials={
            "username": "test",
            "password": "test123",  # pragma: allowlist secret
        },
    )


@patch("viadot.sources.business_core.handle_api_response")
def test_generate_token(mock_api_response, business_core):
    mock_api_response.return_value = Mock(text='{"access_token": "12345"}')
    token = business_core.generate_token()
    t = "12345"
    assert token == t


def test__clean_filters(business_core):
    filters_raw = {
        "BucketCount": 10,
        "BucketNo": 1,
        "FromDate": None,
        "ToDate": None,
    }
    filters_clean = business_core._clean_filters(filters_raw)
    assert filters_clean == {
        "BucketCount": 10,
        "BucketNo": 1,
        "FromDate": "&",
        "ToDate": "&",
    }


def test_to_df(business_core):
    with patch.object(
        business_core,
        "get_data",
        return_value=[{"id": 1, "name": "John Doe"}],
    ):
        df = business_core.to_df()
        assert isinstance(df, pd.DataFrame)
        assert len(df.columns) == 4
        assert len(df) == 1
        assert df["id"].tolist() == [1]
        assert df["name"].tolist() == ["John Doe"]
