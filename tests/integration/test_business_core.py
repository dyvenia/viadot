from unittest.mock import Mock, patch
import pandas as pd
import pytest

from viadot.sources.business_core import BusinessCore


@pytest.fixture(scope="module")
def business_core():
    return BusinessCore(
        url="https://api.businesscore.ae/api/GetCustomerData",
        filters_dict={
            "BucketCount": 10,
            "BucketNo": 1,
            "FromDate": None,
            "ToDate": None,
        },
        credentials={"username": "test", "password": "test123"},
    )


@patch("viadot.sources.business_core.handle_api_response")
def test_generate_token(mock_api_response, business_core):
    mock_api_response.return_value = Mock(text='{"access_token": "12345"}')
    token = business_core.generate_token()
    assert token == "12345"


def test_clean_filters_dict(business_core):
    filters = business_core.clean_filters_dict()
    assert filters == {
        "BucketCount": 10,
        "BucketNo": 1,
        "FromDate": "&",
        "ToDate": "&",
    }


def test_to_df(business_core):
    with patch.object(
        business_core,
        "get_data",
        return_value={"MasterDataList": [{"id": 1, "name": "John Doe"}]},
    ):
        df = business_core.to_df()
        assert isinstance(df, pd.DataFrame)
        assert len(df.columns) == 2
        assert len(df) == 1
        assert df["id"].tolist() == [1]
        assert df["name"].tolist() == ["John Doe"]
