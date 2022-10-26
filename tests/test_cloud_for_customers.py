"""Tests for CloudForCustomers source class"""

from viadot.utils import get_credentials
from viadot.sources.cloud_for_customers import CloudForCustomers
import pandas


def test_cloud_for_customers_to_df():
    url = "https://my336539.crm.ondemand.com/sap/c4c/odata/v1/c4codataapi/"
    credentials_secret = "aia-c4c-qa"
    credentials = get_credentials(credentials_secret)
    c4c = CloudForCustomers(
        url=url,
        credentials=credentials,
    )

    df = c4c.to_df(fields=None, dtype=None)

    assert isinstance(df, pandas.core.frame.DataFrame)
    assert len(df) > 0
