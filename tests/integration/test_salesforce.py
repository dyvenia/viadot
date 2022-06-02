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
        "SAPContactId__c": ["111"],
    }
    df = pd.DataFrame(data=data)
    yield df


def test_upsert_empty(salesforce):
    try:
        df = pd.DataFrame()
        salesforce.upsert(df=df, table="Contact")
    except Exception as exception:
        assert False, exception


def test_upsert(salesforce):
    new_name = "Test Upsert"
    correct_row = [salesforce.download(table="Contact", columns=["Id", "LastName"])[0]]
    to_upsert = pd.DataFrame(correct_row)
    to_upsert["LastName"] = new_name

    try:
        salesforce.upsert(
            df=to_upsert,
            table="Contact",
            raise_on_error=True,
        )
    except Exception as exception:
        assert False, exception
    result = salesforce.to_df(table="Contact", columns=["Id", "LastName"])
    assert len(result.loc[result["LastName"] == new_name]) > 0


def test_upsert_external_id(salesforce, test_df_external):
    try:
        salesforce.upsert(
            df=test_df_external,
            table="Contact",
            external_id="SAPContactId__c",
            raise_on_error=True,
        )
    except Exception as exception:
        assert False, exception
    result = salesforce.to_df(table="Contact", columns=["Id", "LastName"])
    assert len(result.loc[result["LastName"] == "John Tester-External"]) > 0


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
