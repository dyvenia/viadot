import pandas as pd
import pytest
from viadot.tasks import SalesforceUpsert


@pytest.fixture(scope="session")
def test_df():
    data = {
        "Id": ["111"],
        "LastName": ["John Tester-External"],
        "SAPContactId__c": [100551557],
    }
    df = pd.DataFrame(data=data)
    yield df


def test_salesforce_upsert(test_df):
    try:
        sf = SalesforceUpsert()
        sf.run(test_df, table="Contact", external_id="SAPContactId__c")
    except Exception as exception:
        assert False, exception
