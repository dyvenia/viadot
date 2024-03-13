"""Tests for CloudForCustomers source class"""
from datetime import datetime, timedelta

import pandas as pd
import pytest

from viadot.exceptions import CredentialError
from viadot.sources.cloud_for_customers import CloudForCustomers

CONTACT_ENDPOINT = "ContactCollection"
cursor_field = "EntityLastChangedOn"
yesterday = datetime.utcnow() - timedelta(days=10)
cursor_value = yesterday.isoformat(timespec="seconds") + "Z"
cursor_filter = f"{cursor_field} ge datetimeoffset'{cursor_value}'"
filter_params = {"$filter": cursor_filter}


def test_is_configured():
    c4c = CloudForCustomers(
        credentials={"username": "test_user", "password": "test_password"},
    )
    assert c4c


def test_is_configured_throws_credential_error():
    with pytest.raises(CredentialError):
        c4c = CloudForCustomers(
            credentials={"username": "test_user", "password": None},
        )
    with pytest.raises(CredentialError):
        c4c = CloudForCustomers(
            credentials={"username": None, "password": "test_password"},
        )
    with pytest.raises(CredentialError):
        c4c = CloudForCustomers(
            credentials={"username": None, "password": None},
        )


def test_to_df(c4c_config_key):
    c4c = CloudForCustomers(
        config_key=c4c_config_key,
        endpoint=CONTACT_ENDPOINT,
        filter_params=filter_params,
    )

    df = c4c.to_df(fields=None, dtype=None)

    assert isinstance(df, pd.DataFrame)
    assert not df.empty
