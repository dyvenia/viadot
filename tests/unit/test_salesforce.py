import pandas as pd
import pytest
from viadot.sources import Salesforce

TABLE_TO_DOWNLOAD = "Account"
TABLE_TO_UPSERT = "Contact"
TEST_LAST_NAME = "viadot-test"


@pytest.fixture(scope="session")
def salesforce():
    s = Salesforce(config_key="salesforce_dev")
    yield s


@pytest.fixture(scope="session")
def test_row_creation(salesforce):

    # Creating a test row
    test_row = {"LastName": "salesforce-test", "SAPContactId__c": "88111"}
    sf = salesforce.salesforce
    sf.Contact.create(test_row)

    yield

    # Finding the Id of a created row in a table and removing it
    result = sf.query(f"SELECT Id FROM {TABLE_TO_UPSERT} WHERE SAPContactId__c='88111'")
    sf.Contact.delete(result["records"][0]["Id"])


def test_upsert_external_id_correct(salesforce, test_row_creation):

    data = {
        "LastName": [TEST_LAST_NAME],
        "SAPContactId__c": ["88111"],
    }
    df = pd.DataFrame(data=data)

    try:
        salesforce.upsert(df=df, table=TABLE_TO_UPSERT, external_id="SAPContactId__c")
    except Exception as exception:
        raise exception

    sf = salesforce.salesforce
    result = sf.query(
        f"SELECT Id, LastName FROM {TABLE_TO_UPSERT} WHERE SAPContactId__c='88111'"
    )

    assert result["records"][0]["LastName"] == TEST_LAST_NAME


def test_upsert_row_id(salesforce, test_row_creation):

    sf = salesforce.salesforce
    created_row = sf.query(
        f"SELECT Id FROM {TABLE_TO_UPSERT} WHERE SAPContactId__c='88111'"
    )

    created_row_id = created_row["records"][0]["Id"]

    data = {
        "Id": [created_row_id],
        "LastName": [TEST_LAST_NAME],
    }
    df = pd.DataFrame(data=data)

    try:
        salesforce.upsert(df=df, table=TABLE_TO_UPSERT)
    except Exception as exception:
        raise exception

    result = sf.query(
        f"SELECT Id, LastName FROM {TABLE_TO_UPSERT} WHERE Id ='{created_row_id}'"
    )

    assert result["records"][0]["LastName"] == TEST_LAST_NAME


def test_upsert_external_id_wrong(salesforce, test_row_creation):
    data = {
        "LastName": [TEST_LAST_NAME],
        "SAPContactId__c": ["88111"],
    }
    df = pd.DataFrame(data=data)
    with pytest.raises(ValueError):
        salesforce.upsert(df=df, table=TABLE_TO_UPSERT, external_id="SAPId")


def test_download_no_query(salesforce):
    records = salesforce.download(table=TABLE_TO_DOWNLOAD)
    assert len(records) > 0


def test_download_with_query(salesforce):
    query = f"SELECT Id, Name FROM {TABLE_TO_DOWNLOAD}"
    records = salesforce.download(query=query)
    assert len(records) > 0


def test_to_df(salesforce):
    df = salesforce.to_df(table=TABLE_TO_DOWNLOAD)
    print(len(df.values))
    assert df.empty == False
    assert len(df.columns) == 98
    assert len(df.values) >= 1000
