import pandas as pd

from viadot.config import local_config
from viadot.tasks import TM1ToDF

CUBE = local_config.get("test_cube")
VIEW = local_config.get("test_view")


def test_tm1_to_df():
    tm1 = TM1ToDF(CUBE, VIEW)
    df = tm1.run()

    assert isinstance(df, pd.DataFrame)
    assert df.empty is False
