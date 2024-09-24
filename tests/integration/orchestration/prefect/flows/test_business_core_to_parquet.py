from pathlib import Path
from pandas import DataFrame, read_parquet
from viadot.orchestration.prefect.flows import business_core_to_parquet

URL = "https://api.businesscore.ae/api/LappDataIntegrationAPI/GetCustomerData"
PATH = "/home/viadot/data/middle_east/customer_master/customer_master_full_data.parquet"
CREDS = "business-core"


def test_business_core_to_parquet():
    assert not Path(PATH).exists()

    business_core_to_parquet(url=URL, path=PATH, credentials_secret=CREDS, verify=False)

    assert Path(PATH).exists()

    n_cols = 11

    df = read_parquet(PATH)

    assert df.shape[1] == n_cols
    Path(PATH).unlink()
