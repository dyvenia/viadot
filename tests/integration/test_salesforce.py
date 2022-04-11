from asyncio.log import logger
import pytest
import os
import pathlib
import json
import pandas as pd
import configparser
from viadot.exceptions import CredentialError
from viadot.sources import Salesforce
from viadot.config import local_config


@pytest.fixture(scope="session")
def salesforce():
    s = Salesforce()
    yield s


@pytest.fixture(scope="session")
def test_df_external():
    data = {
        "Id": ["111"],
        "LastName": ["John Tester-External"],
        "SAPContactId__c": [100551557],
    }
    df = pd.DataFrame(data=data)
    yield df


@pytest.fixture(scope="session")
def test_df():
    data = {"Id": ["112"], "LastName": ["John Tester-Internal"]}
    df = pd.DataFrame(data=data)
    yield df


def test_upsert_task_empty(salesforce):
    try:
        df = pd.DataFrame()
        salesforce.upsert(df=df, table="Contact")
    except Exception as exception:
        assert False, exception


def test_upsert_task_external_id_correct(salesforce, test_df_external):
    try:
        salesforce.upsert(
            df=test_df_external, table="Contact", external_id="SAPContactId__c"
        )
    except Exception as exception:
        assert False, exception
    result = salesforce.download(table="Contact")
    exists = list(
        filter(lambda contact: contact["LastName"] == "John Tester-External", result)
    )
    assert exists != None


def test_upsert_task_external_id_wrong(salesforce, test_df_external):
    with pytest.raises(ValueError):
        salesforce.upsert(df=test_df_external, table="Contact", external_id="SAPId")


def test_download_task_no_query(salesforce):
    ordered_dict = salesforce.download(table="Account")
    assert len(ordered_dict) > 0


def test_download_task_with_query(salesforce):
    query = "SELECT Id, Name FROM Account"
    ordered_dict = salesforce.download(query=query)
    assert len(ordered_dict) > 0


def test_to_df_task(salesforce):
    df = salesforce.to_df(table="Account")
    assert df.empty == False
