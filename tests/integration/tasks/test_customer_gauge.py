import pandas as pd
import pytest

from viadot.tasks import CustomerGaugeToDF

ENDPOINT = "responses"
CG = CustomerGaugeToDF(endpoint=ENDPOINT)
CUR = 185000
PAGESIZE = 1000


@pytest.mark.looping_api_calls
def test_customer_gauge_to_df_loop():
    df = CG.run(total_load=True, cursor=CUR, pagesize=PAGESIZE)

    assert isinstance(df, pd.DataFrame)
    assert len(df) > PAGESIZE
