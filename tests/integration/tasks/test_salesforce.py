import pandas as pd
import pytest
from viadot.tasks import SalesforceUpsert
from simple_salesforce import SalesforceResourceNotFound


@pytest.fixture(scope="session")
def test_df():
    data = {
        "Id": ["111"],
        "LastName": ["John Tester-External"],
        "SAPContactId__c": ["111"],
    }
    df = pd.DataFrame(data=data)
    yield df


@pytest.fixture(scope="session")
def test_df_wrong():
    data = {
        "Id": ["123"],
        "LastName": ["John Tester-Wrong"],
        "SAPContactId__c": ["111"],
    }
    df = pd.DataFrame(data=data)
    yield df


def test_salesforce_upsert(test_df):
    """
    Id and SAPContactId__c are unique values, you can update only non-unique values for this test.
    If the combiantion of Id and SAPContactId__c do not exist, the test will fail.
    The Id and SAPContactId__c values '111' needs to be replaced with proper one (that exist in the testing system).
    """
    try:
        sf = SalesforceUpsert()
        sf.run(test_df, table="Contact", raise_on_error=True)
    except Exception as exception:
        assert False, exception


def test_salesforce_upsert_incorrect(test_df_wrong):
    """
    Checks if the error handling system catches errors regarding improper IDs.
    """
    with pytest.raises(SalesforceResourceNotFound):
        sf = SalesforceUpsert()
        sf.run(test_df_wrong, table="Contact", raise_on_error=True)


def test_salesforce_upsert_incorrect_warn(test_df_wrong):
    """
    Checks if the error handling system catches errors regarding improper IDs.
    """
    try:
        sf = SalesforceUpsert()
        sf.run(test_df_wrong, table="Contact", raise_on_error=False)
    except Exception as exception:
        assert False, exception


def test_salesforce_upsert_external(test_df):
    """
    Id and SAPContactId__c are unique values, you can update only non-unique values for this test.
    If the combiantion of Id and SAPContactId__c do not exist, the test will fail.
    The Id and SAPContactId__c values '111' needs to be replaced with proper one (that exist in the testing system).
    """
    try:
        sf = SalesforceUpsert()
        sf.run(
            test_df, table="Contact", external_id="SAPContactId__c", raise_on_error=True
        )
    except Exception as exception:
        assert False, exception
