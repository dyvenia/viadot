import pandas as pd
import pytest
from viadot.sources import Salesforce

TABLE_TO_DOWNLOAD = "Account"
TABLE_TO_UPSERT = "Contact"
TEST_LAST_NAME = "viadot-test"

EXPECTED_VALUE = pd.DataFrame(
    {"LastName": ["salesforce-test"], "SAPContactId__c": ["8811111"]}
)

TEST_ROW = {"LastName": "salesforce-test", "SAPContactId__c": "8811111"}

TWO_TEST_ROWS_INSERT = {
    "LastName": ["viadot-insert-1", "viadot-insert-2"],
    "SAPContactId__c": ["8812000", "8812100"],
}

TWO_TEST_ROWS_UPDATE = {
    "LastName": ["viadot-update-1", "viadot-update-2"],
    "SAPContactId__c": ["8812000", "8812100"],
}


def get_tested_records(salesforce, multiple_rows=False):
    sf = salesforce.salesforce
    if multiple_rows:
        result = sf.query(
            f"SELECT Id, LastName FROM {TABLE_TO_UPSERT} WHERE SAPContactId__c in ('8812000','8812100')"
        )
    else:
        result = sf.query(
            f"SELECT Id, LastName FROM {TABLE_TO_UPSERT} WHERE SAPContactId__c='8811111'"
        )

    return result["records"]


@pytest.fixture(scope="session")
def salesforce():

    s = Salesforce(config_key="salesforce_dev")

    yield s

    sf = s.salesforce
    result = sf.query(
        f"SELECT Id FROM {TABLE_TO_UPSERT} WHERE SAPContactId__c in ('8812000','8812100','8811111') "
    )

    # Deletes test rows from Salesforce if any remain
    nr_rows = len(result["records"])
    for nr in range(nr_rows):
        sf.Contact.delete(result["records"][nr]["Id"])


def test_upsert_row_id(salesforce):

    assert not get_tested_records(salesforce)

    sf = salesforce.salesforce
    sf.Contact.create(TEST_ROW)

    data = {
        "Id": [get_tested_records(salesforce)[0]["Id"]],
        "LastName": [TEST_LAST_NAME],
    }
    df = pd.DataFrame(data=data)

    salesforce.upsert(df=df, table=TABLE_TO_UPSERT)

    updated_row = get_tested_records(salesforce)

    assert updated_row[0]["LastName"] == TEST_LAST_NAME

    sf.Contact.delete(updated_row[0]["Id"])


def test_upsert(salesforce):

    assert not get_tested_records(salesforce, multiple_rows=True)

    df_insert = pd.DataFrame(data=TWO_TEST_ROWS_INSERT)

    salesforce.upsert(
        df=df_insert, table="Contact", external_id_column="SAPContactId__c"
    )

    inserted_rows = get_tested_records(salesforce, multiple_rows=True)

    assert inserted_rows[0]["LastName"] == "viadot-insert-1"
    assert inserted_rows[1]["LastName"] == "viadot-insert-2"

    df_update = pd.DataFrame(data=TWO_TEST_ROWS_UPDATE)

    salesforce.upsert(
        df=df_update, table="Contact", external_id_column="SAPContactId__c"
    )

    updated_rows = get_tested_records(salesforce, multiple_rows=True)
    assert updated_rows[0]["LastName"] == "viadot-update-1"
    assert updated_rows[1]["LastName"] == "viadot-update-2"

    sf = salesforce.salesforce
    sf.Contact.delete(inserted_rows[0]["Id"])
    sf.Contact.delete(inserted_rows[1]["Id"])


def test_bulk_upsert(salesforce):

    assert not get_tested_records(salesforce, multiple_rows=True)

    df_insert = pd.DataFrame(data=TWO_TEST_ROWS_INSERT)

    salesforce.bulk_upsert(
        df=df_insert, table="Contact", external_id_column="SAPContactId__c"
    )

    inserted_rows = get_tested_records(salesforce, multiple_rows=True)

    assert inserted_rows[0]["LastName"] == "viadot-insert-1"
    assert inserted_rows[1]["LastName"] == "viadot-insert-2"

    df_update = pd.DataFrame(data=TWO_TEST_ROWS_UPDATE)

    salesforce.bulk_upsert(
        df=df_update, table="Contact", external_id_column="SAPContactId__c"
    )

    updated_rows = get_tested_records(salesforce, multiple_rows=True)
    assert updated_rows[0]["LastName"] == "viadot-update-1"
    assert updated_rows[1]["LastName"] == "viadot-update-2"

    sf = salesforce.salesforce
    sf.Contact.delete(updated_rows[0]["Id"])
    sf.Contact.delete(updated_rows[1]["Id"])


def test_upsert_incorrect_external_id_column(salesforce):
    data = {
        "LastName": [TEST_LAST_NAME],
        "SAPContactId__c": ["8811111"],
    }
    df = pd.DataFrame(data=data)
    with pytest.raises(ValueError):
        salesforce.upsert(df=df, table=TABLE_TO_UPSERT, external_id_column="SAPId")


def test_download_no_query(salesforce):
    records = salesforce.download(table=TABLE_TO_DOWNLOAD)
    assert len(records) > 0


def test_download_with_query(salesforce):
    query = f"SELECT Id, Name FROM {TABLE_TO_DOWNLOAD} LIMIT 10"
    records = salesforce.download(query=query)
    assert len(records) == 10


def test_to_df(salesforce):

    assert not get_tested_records(salesforce)

    sf = salesforce.salesforce
    sf.Contact.create(TEST_ROW)

    df = salesforce.to_df(
        query=f"SELECT LastName, SAPContactId__c FROM {TABLE_TO_UPSERT} WHERE SAPContactId__c='8811111'"
    )

    assert df.equals(EXPECTED_VALUE)

    sf.Contact.delete(get_tested_records(salesforce)[0]["Id"])
