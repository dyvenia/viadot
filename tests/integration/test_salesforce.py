import pandas as pd
import pytest

from viadot.sources import Salesforce


@pytest.fixture(scope="session")
def salesforce():
    s = Salesforce()
    yield s


@pytest.fixture(scope="session")
def test_df_external():
    data = {
        "Id": ["111"],
        "LastName": ["John Tester-External"],
        "SAPContactId__c": ["112"],
    }
    df = pd.DataFrame(data=data)
    yield df


def test_upsert_empty(salesforce):
    try:
        df = pd.DataFrame()
        salesforce.upsert(df=df, table="Contact")
    except Exception as exception:
        assert False, exception


def test_upsert_external_id_correct(salesforce, test_df_external):
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


def test_upsert_external_id_wrong(salesforce, test_df_external):
    with pytest.raises(ValueError):
        salesforce.upsert(df=test_df_external, table="Contact", external_id="SAPId")


def test_download_no_query(salesforce):
    ordered_dict = salesforce.download(table="Account")
    assert len(ordered_dict) > 0


def test_download_with_query(salesforce):
    query = "SELECT Id, Name FROM Account"
    ordered_dict = salesforce.download(query=query)
    assert len(ordered_dict) > 0


def test_to_df(salesforce):
    df = salesforce.to_df(table="Account")
    assert df.empty == False
