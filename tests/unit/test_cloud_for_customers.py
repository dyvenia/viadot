"""Tests for CloudForCustomers source class"""
import pandas as pd

from viadot.sources.cloud_for_customers import CloudForCustomers

CONTACT_ENDPOINT = "ContactCollection"
cursor_field = "EntityLastChangedOn"
cursor_value = "2023-04-30T00:00:00Z"
cursor_filter = f"{cursor_field} ge datetimeoffset'{cursor_value}'"
filter_params = {"$filter": cursor_filter}


def test_cloud_for_customers_to_df(c4c_config_key):
    c4c = CloudForCustomers(
        config_key=c4c_config_key,
        endpoint=CONTACT_ENDPOINT,
        filter_params=filter_params,
    )

    df = c4c.to_df(fields=None, dtype=None)

    assert isinstance(df, pd.DataFrame)
    assert not df.empty
