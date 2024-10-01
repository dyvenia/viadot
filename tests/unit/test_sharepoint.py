from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest
import sharepy
from sharepy.errors import AuthError

from viadot.exceptions import CredentialError
from viadot.sources import Sharepoint
from viadot.sources.sharepoint import SharepointCredentials


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

    def _download_file_stream(self, url: str | None = None, **kwargs):  # noqa: ARG002
        if "nrows" in kwargs:
            msg = "Parameter 'nrows' is not supported."
            raise ValueError(msg)

        return pd.ExcelFile(Path("tests/unit/test_file.xlsx"))


@pytest.fixture
def sharepoint_mock():
    return SharepointMock(credentials=DUMMY_CREDS)


def test_valid_credentials():
    credentials = {
        "site": "tenant.sharepoint.com",
        "username": "user@example.com",
        "password": "password",
    }
    shrp_creds = SharepointCredentials(**credentials)
    assert shrp_creds.site == credentials["site"]
    assert shrp_creds.username == credentials["username"]
    assert shrp_creds.password == credentials["password"]


def test_invalid_authentication():
    credentials = {
        "site": "tenant.sharepoint.com",
        "username": "user@example.com",
        "password": "password",
    }

    s = Sharepoint(credentials=credentials)

    # Patch the sharepy.connect method to simulate an authentication failure
    with patch("sharepy.connect") as mock_connect:
        mock_connect.side_effect = AuthError("Authentication failed")

        with pytest.raises(
            CredentialError,
            match="Could not authenticate to tenant.sharepoint.com with provided credentials.",
        ):
            s.get_connection()


def test_missing_username():
    credentials = {"site": "example.sharepoint.com", "password": "password"}
    with pytest.raises(
        CredentialError,
        match="'site', 'username', and 'password' credentials are required.",
    ):
        SharepointCredentials(**credentials)


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
    dir_ext = sharepoint_mock._get_file_extension(url=url_dir)

    assert excel_ext == ".xlsx"
    assert txt_ext == ".txt"
    assert dir_ext == ""


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


def test__load_and_parse_not_valid_extension(sharepoint_mock):
    with pytest.raises(ValueError):  # noqa: PT011
        sharepoint_mock._load_and_parse(file_url="https://example.com/file.txt")


def test_scan_sharepoint_folder_valid_url(sharepoint_mock):
    url = "https://company.sharepoint.com/sites/site_name/final_folder/"

    # Mock the response from SharePoint
    mock_response = MagicMock()
    mock_response.json.return_value = {
        "d": {
            "results": [
                {"Name": "file1.txt"},
                {"Name": "file2.txt"},
            ]
        }
    }

    # Inject the mock response
    sharepoint_mock.get_connection().get = MagicMock(return_value=mock_response)

    expected_files = [
        "https://company.sharepoint.com/sites/site_name/final_folder/file1.txt",
        "https://company.sharepoint.com/sites/site_name/final_folder/file2.txt",
    ]

    result = sharepoint_mock.scan_sharepoint_folder(url)
    assert result == expected_files


def test_scan_sharepoint_folder_invalid_url(sharepoint_mock):
    url = "https://company.sharepoint.com/folder/sub_folder/final_folder"

    with pytest.raises(ValueError, match="URL does not contain '/sites/' segment."):
        sharepoint_mock.scan_sharepoint_folder(url)


def test_scan_sharepoint_folder_empty_response(sharepoint_mock):
    url = (
        "https://company.sharepoint.com/sites/site_name/folder/sub_folder/final_folder"
    )

    mock_response = MagicMock()
    mock_response.json.return_value = {"d": {"results": []}}

    sharepoint_mock.get_connection().get = MagicMock(return_value=mock_response)

    result = sharepoint_mock.scan_sharepoint_folder(url)
    assert result == []


def test_download_file_stream_unsupported_param(sharepoint_mock):
    url = "https://company.sharepoint.com/sites/site_name/folder/test_file.xlsx"

    with pytest.raises(ValueError, match="Parameter 'nrows' is not supported."):
        sharepoint_mock._download_file_stream(url, nrows=10)
