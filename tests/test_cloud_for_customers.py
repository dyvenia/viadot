"""Tests for CloudForCustomers source class"""

from viadot.utils import get_credentials
from viadot.sources.cloud_for_customers import CloudForCustomers
import pandas as pd


url = "https://my336539.crm.ondemand.com/sap/c4c/odata/v1/c4codataapi/"
CONTACT_ENDPOINT = "ContactCollection"

cursor_field = "EntityLastChangedOn"
cursor_value = "2022-10-30T00:00:00Z"
cursor_filter = f"{cursor_field} ge datetimeoffset'{cursor_value}'"
filter_params = {"$filter": cursor_filter}


def test_cloud_for_customers_to_df():
    credentials_secret = "aia-c4c-qa"
    credentials = get_credentials(credentials_secret)
    c4c = CloudForCustomers(
        url=url,
        endpoint=CONTACT_ENDPOINT,
        filter_params=filter_params,
        credentials=credentials,
    )

    df = c4c.to_df(fields=None, dtype=None)

    assert isinstance(df, pd.core.frame.DataFrame)
    assert len(df) > 0
