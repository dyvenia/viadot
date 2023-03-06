import pandas as pd
import pytest
from viadot.sources import Salesforce

TABLE_TO_DOWNLOAD = "Account"
TABLE_TO_UPSERT = "Contact"
TEST_LAST_NAME = "viadot-test"
ID_TO_UPSERT = "0035E00001YGWK3QAP"


@pytest.fixture(scope="session")
def salesforce():
    s = Salesforce(config_key="salesforce_dev")
    yield s


@pytest.fixture(scope="session")
def test_df_data(salesforce):
    data = {
        "Id": [ID_TO_UPSERT],
        "LastName": [TEST_LAST_NAME],
    }
    df = pd.DataFrame(data=data)

    yield df

    sf = salesforce.salesforce
    sf.Contact.update(ID_TO_UPSERT, {"LastName": "LastName"})


@pytest.fixture(scope="session")
def test_df_external(salesforce):
    data = {
        "LastName": [TEST_LAST_NAME],
        "SAPContactId__c": ["111"],
    }
    df = pd.DataFrame(data=data)
    yield df

    sf = salesforce.salesforce
    sf.Contact.update(ID_TO_UPSERT, {"LastName": "LastName"})


def test_upsert_external_id_correct(salesforce, test_df_external):
    try:
        salesforce.upsert(
            df=test_df_external, table=TABLE_TO_UPSERT, external_id="SAPContactId__c"
        )
    except Exception as exception:
        raise exception

    sf = salesforce.salesforce
    result = sf.query(
        f"SELECT ID, LastName FROM {TABLE_TO_UPSERT} WHERE ID='{ID_TO_UPSERT}'"
    )

    assert result["records"][0]["LastName"] == TEST_LAST_NAME


def test_upsert_external_id_wrong(salesforce, test_df_external):
    with pytest.raises(ValueError):
        salesforce.upsert(
            df=test_df_external, table=TABLE_TO_UPSERT, external_id="SAPId"
        )


def test_download_no_query(salesforce):
    ordered_dict = salesforce.download(table=TABLE_TO_DOWNLOAD)
    assert len(ordered_dict) > 0


def test_download_with_query(salesforce):
    query = f"SELECT Id, Name FROM {TABLE_TO_DOWNLOAD}"
    ordered_dict = salesforce.download(query=query)
    assert len(ordered_dict) > 0


def test_to_df(salesforce):
    df = salesforce.to_df(table=TABLE_TO_DOWNLOAD)
    print(len(df.values))
    assert df.empty == False
    assert len(df.columns) == 98
    assert len(df.values) >= 1000


def test_upsert_row_id(salesforce, test_df_data):
    try:
        salesforce.upsert(df=test_df_data, table=TABLE_TO_UPSERT)
    except Exception as exception:
        raise exception

    sf = salesforce.salesforce
    result = sf.query(
        f"SELECT ID, LastName FROM {TABLE_TO_UPSERT} WHERE ID='{ID_TO_UPSERT}'"
    )

    assert result["records"][0]["LastName"] == TEST_LAST_NAME
