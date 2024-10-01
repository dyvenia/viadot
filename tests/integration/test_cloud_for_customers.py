"""Tests for CloudForCustomers source class."""

from datetime import datetime, timedelta

import pandas as pd

from viadot.sources.cloud_for_customers import CloudForCustomers


CONTACT_ENDPOINT = "ContactCollection"
cursor_field = "EntityLastChangedOn"
yesterday = datetime.utcnow() - timedelta(days=10)
cursor_value = yesterday.isoformat(timespec="seconds") + "Z"
cursor_filter = f"{cursor_field} ge datetimeoffset'{cursor_value}'"
filter_params = {"$filter": cursor_filter}


def test_to_df(c4c_config_key):
    c4c = CloudForCustomers(
        config_key=c4c_config_key,
        endpoint=CONTACT_ENDPOINT,
        filter_params=filter_params,
    )

    df = c4c.to_df(fields=None, dtype=None)

    assert isinstance(df, pd.DataFrame)
    assert not df.empty
