from io import BytesIO
from unittest.mock import MagicMock, patch

from openpyxl import Workbook
import pandas as pd
import pytest
import requests
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


def create_excel_file():
    """Create a simple Excel file in memory for testing."""
    wb = Workbook()
    ws = wb.active
    ws.title = "Sheet1"
    ws["A1"] = "col_a"
    ws["B1"] = "col_b"
    ws["A2"] = "val1"
    ws["B2"] = "val1"
    ws["A3"] = ""
    ws["B3"] = "val2"
    ws["A4"] = "val2"
    ws["B4"] = "val3"
    ws["A5"] = "NA"
    ws["B5"] = "val4"
    ws["A6"] = "N/A"
    ws["B6"] = "val5"
    ws["A7"] = "#N/A"
    ws["B7"] = "val6"
    bio = BytesIO()
    wb.save(bio)
    bio.seek(0)
    return bio.getvalue()


class SharepointMock(Sharepoint):
    def get_connection(self):
        return sharepy.session.SharePointSession

    def _download_file_stream(self, url: str | None = None, **kwargs):  # noqa: ARG002
        if "nrows" in kwargs:
            msg = "Parameter 'nrows' is not supported."
            raise ValueError(msg)

        return pd.ExcelFile(BytesIO(create_excel_file()))


@pytest.fixture
def sharepoint_mock():
    return SharepointMock(credentials=DUMMY_CREDS)


@pytest.fixture
def sharepoint():
    credentials = {
        "site": "https://example.sharepoint.com",
        "username": "email@example.com",
        "password": "password",
    }
    return Sharepoint(credentials=credentials)


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


def test_successful_download(sharepoint):
    url = "https://example.sharepoint.com/sites/site/Shared%20Documents/file.xlsx"
    with patch("sharepy.connect") as mock_connect:
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.content = create_excel_file()
        mock_session = MagicMock()
        mock_session.get.return_value = mock_response
        mock_connect.return_value = mock_session
        df = sharepoint.to_df(url)
        assert not df.empty
        assert len(df) == 6


def test_access_denied(sharepoint):
    url = "https://example.sharepoint.com/sites/site/Shared%20Documents/restricted_file.xlsx"
    with patch("sharepy.connect") as mock_connect:
        mock_response = MagicMock()
        mock_response.status_code = 403
        mock_response.raise_for_status.side_effect = requests.exceptions.HTTPError(
            response=mock_response
        )
        mock_session = MagicMock()
        mock_session.get.return_value = mock_response
        mock_connect.return_value = mock_session
        with pytest.raises(requests.exceptions.HTTPError):
            sharepoint.to_df(url)


def test_invalid_excel_file(sharepoint):
    url = "https://example.sharepoint.com/sites/site/Shared%20Documents/file.xlsx"
    with patch("sharepy.connect") as mock_connect:
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.content = b"not an excel file"
        mock_session = MagicMock()
        mock_session.get.return_value = mock_response
        mock_connect.return_value = mock_session
        with pytest.raises(ValueError, match="Excel file format cannot be determined"):
            sharepoint.to_df(url)


def test_mixed_file_extensions(sharepoint):
    url = "https://example.sharepoint.com/sites/site/Shared%20Documents/folder"
    with (
        patch("sharepy.connect") as mock_connect,
        patch.object(Sharepoint, "scan_sharepoint_folder") as mock_scan,
    ):
        mock_scan.return_value = [
            url + "/file1.xlsx",
            url + "/file2.txt",
            url + "/file3.pdf",
        ]
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.content = create_excel_file()
        mock_session = MagicMock()
        mock_session.get.return_value = mock_response
        mock_connect.return_value = mock_session

        # Mock _load_and_parse to track which files are processed
        with patch.object(Sharepoint, "_load_and_parse") as mock_load_and_parse:
            mock_load_and_parse.return_value = pd.DataFrame(
                {
                    "col_a": ["val1", "", "val2", "NA", "N/A", "#N/A"],
                    "col_b": ["val1", "val2", "val3", "val4", "val5", "val6"],
                }
            )
            df = sharepoint.to_df(url)

            # Verify that _load_and_parse was only called for .xlsx files
            mock_load_and_parse.assert_called_once_with(
                file_url=url + "/file1.xlsx",
                sheet_name=None,
                na_values=None,
                **{},
            )

        assert not df.empty
        assert len(df) == 6


def test_empty_folder(sharepoint):
    url = "https://example.sharepoint.com/sites/site/Shared%20Documents/folder"
    with (
        patch("sharepy.connect"),
        patch.object(Sharepoint, "scan_sharepoint_folder") as mock_scan,
    ):
        mock_scan.return_value = []
        mock_session = MagicMock()
        mock_connect = patch("sharepy.connect", return_value=mock_session)
        mock_connect.start()
        df = sharepoint.to_df(url)
        mock_connect.stop()
        assert df.empty
