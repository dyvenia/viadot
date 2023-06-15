import pandas as pd

from viadot.tasks.mediatool import MediatoolToDF


def test_join_df():
    data_left = {"id": [102, 112], "amount": [33, 48]}
    df_left = pd.DataFrame.from_dict(data=data_left)
    data_right = {"id_r": [103, 112], "prize": [12, 234], "name": ["cat", "dog"]}
    df_right = pd.DataFrame.from_dict(data=data_right)

    mtool = MediatoolToDF(mediatool_credentials_key="MEDIATOOL-TESTS")

    df = mtool.join_dfs(
        df_left=df_left,
        df_right=df_right,
        left_on="id",
        right_on="id_r",
        columns_from_right_df=["id_r", "prize"],
        how="left",
    )

    assert isinstance(df, pd.DataFrame)
    assert "prize" in df.columns
    assert not df.empty
