from pathlib import Path

import pandas as pd
import pytest
import sharepy
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
    def get_connection(self):
        return sharepy.session.SharePointSession

    def _download_file_stream(self, url=None):
        return pd.ExcelFile(Path("tests/unit/test_file.xlsx"))


@pytest.fixture
def sharepoint_mock():
    return SharepointMock(credentials=DUMMY_CREDS)


def test_sharepoint_default_na(sharepoint_mock):
    df = sharepoint_mock.to_df(
        url="test/file.xlsx", na_values=Sharepoint.DEFAULT_NA_VALUES
    )

    assert not df.empty
    assert "NA" not in list(df["col_a"])


def test_sharepoint_custom_na(sharepoint_mock):
    df = sharepoint_mock.to_df(
        url="test/file.xlsx",
        na_values=[v for v in Sharepoint.DEFAULT_NA_VALUES if v != "NA"],
    )

    assert not df.empty
    assert "NA" in list(df["col_a"])


def test__get_file_extension(sharepoint_mock):
    url_excel = "https://tenant.sharepoint.com/sites/site/file.xlsx"
    url_dir = "https://tenant.sharepoint.com/sites/site/"
    url_txt = "https://tenant.sharepoint.com/sites/site/file.txt"

    excel_ext = sharepoint_mock._get_file_extension(url=url_excel)
    txt_ext = sharepoint_mock._get_file_extension(url=url_txt)
    dir = sharepoint_mock._get_file_extension(url=url_dir)

    assert excel_ext == ".xlsx"
    assert txt_ext == ".txt"
    assert dir == ""


def test__is_file(sharepoint_mock):
    is_file = sharepoint_mock._is_file(url="https://example.com/file.xlsx")
    assert is_file is True

    is_file = sharepoint_mock._is_file(url="https://example.com/dir")
    assert is_file is False


def test__parse_excel_single_sheet(sharepoint_mock):
    excel_file = sharepoint_mock._download_file_stream()
    result_df = sharepoint_mock._parse_excel(excel_file, sheet_name="Sheet1")
    expected = pd.DataFrame(
        {
            "col_a": ["val1", "", "val2", "NA", "N/A", "#N/A"],
            "col_b": ["val1", "val2", "val3", "val4", "val5", "val6"],
        }
    )

    assert result_df["col_b"].equals(expected["col_b"])


def test__parse_excel_string_dtypes(sharepoint_mock):
    excel_file = sharepoint_mock._download_file_stream()
    result_df = sharepoint_mock._parse_excel(excel_file, sheet_name="Sheet1")

    for column in result_df.columns:
        assert result_df[column].dtype == object
