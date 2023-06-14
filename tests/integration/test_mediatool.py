import json
from typing import List

import pandas as pd
import pytest
from prefect.tasks.secrets import PrefectSecret

from viadot.exceptions import APIError
from viadot.sources import Mediatool
from viadot.task_utils import credentials_loader

CREDENTIALS = credentials_loader.run(credentials_secret="MEDIATOOL-TESTS")
MTOOL = Mediatool(credentials=CREDENTIALS)


def test_get_campaigns_df():
    camps = MTOOL.get_campaigns(CREDENTIALS["ORG"])
    assert isinstance(camps, pd.DataFrame)


def test_get_campaigns_dict():
    camps = MTOOL.get_campaigns(CREDENTIALS["ORG"], return_dataframe=False)
    assert isinstance(camps, list)
    assert isinstance(camps[0], dict)


def test_get_organizations():
    orgs = MTOOL.get_organizations(user_id=CREDENTIALS["USER_ID"])
    assert isinstance(orgs, pd.DataFrame)


def test_get_media_entries():
    media_entries = MTOOL.get_media_entries(
        organization_id=CREDENTIALS["ORG"], columns=["_id"]
    )
    assert isinstance(media_entries, pd.DataFrame)


def test_get_media_types_correct_id():
    media_types = MTOOL.get_media_types(media_type_ids=[CREDENTIALS["MEDIA_TYPE_ID"]])
    assert isinstance(media_types, pd.DataFrame)


def test_get_media_types_wrong_id():
    with pytest.raises(
        APIError, match=r"Perhaps your account credentials need to be refreshed"
    ):
        _ = MTOOL.get_media_types(["040404"])


def test_get_vehicles(caplog):
    _ = MTOOL.get_vehicles(vehicle_ids=["100000", "200000"])
    assert "Vehicle were not found for: ['100000', '200000']" in caplog.text


def test_rename_columns_correct():
    data = {"id": [1, 2], "amount": [3, 4]}
    df = pd.DataFrame(data=data)
    df_updated = MTOOL.rename_columns(df=df, column_suffix="source_test")

    assert isinstance(df_updated, pd.DataFrame)
    assert "id_source_test" in df_updated.columns


def test_rename_columns_not_df():
    with pytest.raises(TypeError, match=r"is not a data frame."):
        _ = MTOOL.rename_columns(df="DF", column_suffix="source_test")


def test_rename_columns_empty_source_name():
    data = {"id": [1, 2], "amount": [3, 4]}
    df = pd.DataFrame(data=data)
    df_updated = MTOOL.rename_columns(df=df)

    assert isinstance(df_updated, pd.DataFrame)
    assert "id_rename" in df_updated.columns
