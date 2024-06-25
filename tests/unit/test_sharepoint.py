from pathlib import Path

import pandas as pd
from viadot.sources import Sharepoint

DUMMY_CREDS = {"site": "test", "username": "test2", "password": "test"}
SAMPLE_DF = pd.DataFrame(
    {
        "int_col": [1, 2, 3, 4, 5, None],
        "float_col": [1.1, 2.2, 3.3, 3.0, 5.5, 6.6],
        "str_col": ["a", "b", "c", "d", "e", "f"],
        "nan_col": [None, None, None, None, None, None],
        "mixed_col": [1, "text", None, None, 4.2, "text2"],
    }
)


class SharepointMock(Sharepoint):
    def _download_excel(self, url=None):
        return pd.ExcelFile(Path("tests/unit/test_file.xlsx"))


def test_sharepoint_default_na():
    s = SharepointMock(credentials=DUMMY_CREDS)
    df = s.to_df(url="test", na_values=Sharepoint.DEFAULT_NA_VALUES)

    assert not df.empty
    assert "NA" not in list(df["col_a"])


def test_sharepoint_custom_na():
    s = SharepointMock(credentials=DUMMY_CREDS)
    df = s.to_df(
        url="test", na_values=[v for v in Sharepoint.DEFAULT_NA_VALUES if v != "NA"]
    )

    assert not df.empty
    assert "NA" in list(df["col_a"])


def test_sharepoint_convert_all_to_string_type():
    s = SharepointMock(credentials=DUMMY_CREDS)
    converted_df = s._convert_all_to_string_type(df=SAMPLE_DF)

    assert not converted_df.empty
    assert pd.isnull(converted_df["nan_col"]).all()


def test_sharepoint_convert_empty_columns_to_string():
    s = SharepointMock(credentials=DUMMY_CREDS)
    converted_df = s._empty_column_to_string(df=SAMPLE_DF)

    assert not converted_df.empty
    assert converted_df["float_col"].dtype == float
    assert converted_df["nan_col"].dtype == "string"
