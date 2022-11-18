"""Tests for CloudForCustomers source class"""
from viadot.config import get_source_credentials
from viadot.sources.cloud_for_customers import CloudForCustomers
import pandas as pd


CONTACT_ENDPOINT = "ContactCollection"
cursor_field = "EntityLastChangedOn"
cursor_value = "2022-10-30T00:00:00Z"
cursor_filter = f"{cursor_field} ge datetimeoffset'{cursor_value}'"
filter_params = {"$filter": cursor_filter}


def test_cloud_for_customers_to_df(TEST_C4C_API_URL):
    credentials_secret = "aia-c4c-qa"
    credentials = get_source_credentials(credentials_secret)
    c4c = CloudForCustomers(
        url=TEST_C4C_API_URL,
        endpoint=CONTACT_ENDPOINT,
        filter_params=filter_params,
        credentials=credentials,
    )

    df = c4c.to_df(fields=None, dtype=None)

    assert isinstance(df, pd.DataFrame)
    assert not df.empty
