"""Tests for CloudForCustomers source class"""

from viadot.utils import get_credentials
from viadot.sources.cloud_for_customers import CloudForCustomers
import pandas


url = "https://my336539.crm.ondemand.com/sap/c4c/odata/v1/c4codataapi/"
CONTACT_ENDPOINT = "ContactCollection"

cursor_value = "2022-10-30T00:00:00Z"
cursor_field = "EntityLastChangedOn"
cursor_filter = f"{cursor_field} ge datetimeoffset'{cursor_value}'"


def test_cloud_for_customers_to_df():
    credentials_secret = "aia-c4c-qa"
    credentials = get_credentials(credentials_secret)

    c4c = CloudForCustomers(
        url=url,
        endpoint=CONTACT_ENDPOINT,
        credentials=credentials,
        cursor_filter=cursor_filter,
    )

    df = c4c.to_df(fields=None, dtype=None)

    assert isinstance(df, pandas.core.frame.DataFrame)
    assert len(df) > 0
