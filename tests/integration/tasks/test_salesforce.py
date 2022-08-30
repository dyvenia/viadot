import pandas as pd
import pytest

from viadot.tasks import SalesforceToDF, SalesforceUpsert


@pytest.fixture(scope="session")
def test_df():
    data = {
        "Id": ["111"],
        "LastName": ["John Tester-External 3"],
        "SAPContactId__c": [111],
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
        sf.run(test_df, table="Contact")
    except Exception as exception:
        assert False, exception


def test_salesforce_to_df():
    sf_to_df = SalesforceToDF(
        query="SELECT IsDeleted, FiscalYear FROM Opportunity LIMIT 50"
    )
    df = sf_to_df.run()

    assert isinstance(df, pd.DataFrame)
    assert (50, 2) == df.shape
