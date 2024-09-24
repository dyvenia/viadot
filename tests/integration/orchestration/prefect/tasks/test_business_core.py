from pathlib import Path
from pandas import DataFrame

from prefect import flow
from viadot.orchestration.prefect.tasks.business_core import business_core_to_df

URL = "https://api.businesscore.ae/api/LappDataIntegrationAPI/GetCustomerData"
PATH = "/home/viadot/data/middle_east/customer_master/customer_master_full_data.parquet"
CREDS = "business-core"


def test_business_core_to_df():
    @flow
    def test_flow():
        return business_core_to_df(
            url=URL, path=PATH, credentials_secret=CREDS, verify=False
        )

    df = test_flow()
    assert isinstance(df, DataFrame)

    n_cols = 11
    assert df.shape[1] == n_cols
