from pathlib import Path

import pandas as pd
from viadot.sources import Sharepoint


class SharepointMock(Sharepoint):
    def _download_excel(self, url=None):
        return pd.ExcelFile(Path("tests/unit/test_file.xlsx"))


def test_sharepoint_default_na():
    dummy_creds = {"site": "test", "username": "test2", "password": "test"}
    na_values1 = Sharepoint.DEFAULT_NA_VALUES

    s = SharepointMock(credentials=dummy_creds)
    df = s.to_df(url="test", na_values=na_values1)

    assert not df.empty
    assert "NA" not in list(df["col_a"])


def test_sharepoint_custom_na():
    dummy_creds = {"site": "test", "username": "test", "password": "test"}
    na_values2 = Sharepoint.DEFAULT_NA_VALUES

    s = SharepointMock(credentials=dummy_creds)
    df = s.to_df(url="test", na_values=na_values2.remove("NA"))

    assert not df.empty
    assert "NA" in list(df["col_a"])
