import logging
from pathlib import Path
from unittest.mock import MagicMock, mock_open, patch

import pendulum
from pydantic import SecretStr
import pytest

from viadot.exceptions import CredentialError
from viadot.sources import SMB


SERVER_PATH = "//server/test_folder_path"
TODAY_DATE = pendulum.today().date()


@pytest.fixture
def valid_credentials():
    return {
        "username": "default@example.com",
        "password": SecretStr("default_password"),  # pragma: allowlist secret
    }


@pytest.fixture
def smb_instance(valid_credentials):
    return SMB(base_path=SERVER_PATH, credentials=valid_credentials)


@pytest.fixture
def mock_smb_dir_entry_file():
    mock = MagicMock()
    mock.name = "test_file.txt"
    mock.path = f"{SERVER_PATH}/test_file.txt"
    mock.is_dir.return_value = False
    mock.is_file.return_value = True
    mock.stat.return_value.st_ctime = pendulum.now().timestamp()
    return mock


@pytest.fixture
def mock_smb_dir_entry_dir():
    mock = MagicMock()
    mock.name = "test_folder_path"
    mock.path = SERVER_PATH
    mock.is_dir.return_value = True
    mock.is_file.return_value = False
    return mock


def test_smb_initialization_with_credentials(valid_credentials):
    smb = SMB(base_path=SERVER_PATH, credentials=valid_credentials)
    assert smb.credentials["username"] == "default@example.com"
    assert smb.credentials["password"] == SecretStr("default_password")


def test_smb_initialization_without_credentials():
    with pytest.raises(
        CredentialError,
        match="`username`, and `password` credentials are required.",
    ):
        SMB(base_path=SERVER_PATH)


@pytest.mark.parametrize(
    ("filename_regex", "extensions", "date_filter"),
    [
        ([None], None, "<<pendulum.yesterday().date()>>"),
        (["keyword1"], None, "<<pendulum.yesterday().date()>>"),
        ([None], [".txt"], "<<pendulum.yesterday().date()>>"),
        (["keyword1"], [".txt"], "<<pendulum.yesterday().date()>>"),
    ],
)
def test_scan_and_store(smb_instance, filename_regex, extensions, date_filter):
    with (
        patch.object(smb_instance, "_scan_directory") as mock_scan_directory,
        patch.object(smb_instance, "_parse_dates") as mock_parse_dates,
    ):
        mock_date_result = (
            pendulum.yesterday().date() if isinstance(date_filter, str) else None
        )
        mock_parse_dates.return_value = mock_date_result

        smb_instance.scan_and_store(
            filename_regex=filename_regex,
            extensions=extensions,
            date_filter=date_filter,
        )

        mock_parse_dates.assert_called_once_with(
            date_filter=date_filter,
            dynamic_date_symbols=["<<", ">>"],
            dynamic_date_format="%Y-%m-%d",
            dynamic_date_timezone="UTC",
        )

        mock_scan_directory.assert_called_once_with(
            path=smb_instance.base_path,
            filename_regex=filename_regex,
            extensions=extensions,
            date_filter_parsed=mock_date_result,
        )


def test_scan_and_store_basic(smb_instance, mock_smb_dir_entry_file):
    with (
        patch("smbclient.scandir") as mock_scandir,
        patch.object(smb_instance, "_get_file_content") as mock_get_content,
        patch.object(smb_instance, "_is_matching_file") as mock_is_matching,
    ):
        mock_scandir.return_value = [mock_smb_dir_entry_file]
        mock_file_content = b"Test content"
        mock_get_content.return_value = {
            mock_smb_dir_entry_file.path: mock_file_content
        }
        mock_is_matching.return_value = True

        result = smb_instance.scan_and_store()

        assert isinstance(result, dict)
        assert len(result) == 1, f"Expected 1 file, got {len(result)}"
        assert mock_smb_dir_entry_file.path in result
        assert result[mock_smb_dir_entry_file.path] == mock_file_content


def test_scan_directory_recursive_search(
    smb_instance, mock_smb_dir_entry_dir, mock_smb_dir_entry_file
):
    with (
        patch("smbclient.scandir") as mock_scandir,
        patch.object(smb_instance, "_get_file_content") as mock_get_content,
        patch.object(smb_instance, "_is_matching_file") as mock_is_matching,
    ):
        # Configure directory structure
        root_dir = mock_smb_dir_entry_dir
        root_dir.path = SERVER_PATH

        sub_dir = MagicMock()
        sub_dir.name = "subdir"
        sub_dir.path = f"{SERVER_PATH}/subdir"
        sub_dir.is_dir.return_value = True
        sub_dir.is_file.return_value = False

        nested_file = mock_smb_dir_entry_file
        nested_file.path = f"{SERVER_PATH}/subdir/file.txt"

        mock_scandir.side_effect = [
            [root_dir],  # Initial root directory scan
            [sub_dir],  # First subdirectory scan
            [nested_file],  # Final nested directory scan
        ]

        mock_is_matching.return_value = True
        mock_file_content = b"Recursive content"
        mock_get_content.return_value = {nested_file.path: mock_file_content}

        # Execute the scan starting at root
        result = smb_instance._scan_directory(
            path=SERVER_PATH,
            filename_regex=None,
            extensions=None,
            date_filter_parsed=None,
        )

        # Verify scanning sequence
        assert mock_scandir.call_count == 3, "Should scan 3 levels deep"
        assert {call[0][0] for call in mock_scandir.call_args_list} == {
            SERVER_PATH,
            f"{SERVER_PATH}/subdir",
        }

        assert isinstance(result, dict)
        assert len(result) == 1, f"Expected 1 file, got {len(result)}. Result: {result}"
        assert nested_file.path in result
        assert result[nested_file.path] == mock_file_content
        mock_is_matching.assert_any_call(nested_file, None, None, None)


@patch("smbclient.scandir")
def test_empty_directory_scan(mock_scandir, smb_instance):
    """Test scanning empty directory structure."""
    mock_scandir.side_effect = lambda path: {
        "/empty": [],
    }.get(path, [])

    smb_instance._handle_matching_file = MagicMock()
    smb_instance._scan_directory("/empty", None, None, None)

    smb_instance._handle_matching_file.assert_not_called()
    mock_scandir.assert_called_once_with("/empty")


def test_scan_directory_error_handling(smb_instance):
    with (
        patch.object(smb_instance, "_get_directory_entries") as mock_get_entries,
        patch.object(smb_instance, "logger") as mock_logger,
    ):
        mock_get_entries.side_effect = Exception("Test error")

        smb_instance._scan_directory(SERVER_PATH, None, None)

        mock_logger.exception.assert_called_once_with(
            f"Error scanning or downloading from {SERVER_PATH}: Test error"
        )


def test_parse_dates_single_date(smb_instance):
    date_filter_keyword = "<<yesterday>>"
    result = smb_instance._parse_dates(date_filter=date_filter_keyword)

    assert result == pendulum.yesterday().date()

    date_filter_date = "<<pendulum.yesterday()>>"
    result = smb_instance._parse_dates(date_filter=date_filter_date)

    assert result == pendulum.yesterday().date()


def test_parse_dates_date_range(smb_instance):
    date_filter_yesterday = "<<yesterday>>"
    date_filter_today = "<<today>>"

    start_date, end_date = smb_instance._parse_dates(
        date_filter=(date_filter_yesterday, date_filter_today)
    )

    assert start_date == pendulum.yesterday().date()
    assert end_date == pendulum.today().date()


def test_parse_dates_none(smb_instance):
    date_filter = None
    result = smb_instance._parse_dates(date_filter=date_filter)

    assert result is None


def test_parse_dates_wrong_input(smb_instance):
    date_filter = ["<<pendulum.today()>>"]

    with pytest.raises(
        ValueError,
        match="date_filter must be a string, a tuple of exactly 2 dates, or None.",
    ):
        smb_instance._parse_dates(date_filter=date_filter)


def test_get_directory_entries(
    smb_instance, mock_smb_dir_entry_dir, mock_smb_dir_entry_file
):
    expected_entries = [mock_smb_dir_entry_file, mock_smb_dir_entry_dir]

    with patch("smbclient.scandir") as mock_scandir:
        mock_scandir.return_value = expected_entries

        entries = smb_instance._get_directory_entries(path=SERVER_PATH)

        result_entries = list(entries)

        assert result_entries == expected_entries
        assert len(result_entries) == 2

        # Check file entry
        assert result_entries[0].is_file()
        assert not result_entries[0].is_dir()
        assert result_entries[0].name == "test_file.txt"
        assert isinstance(result_entries[0].stat().st_ctime, float)

        # Check directory entry
        assert result_entries[1].is_dir()
        assert not result_entries[1].is_file()
        assert result_entries[1].name == "test_folder_path"

        assert all(entry.path.startswith(SERVER_PATH) for entry in result_entries)


@pytest.mark.parametrize(
    (
        "is_dir",
        "name",
        "filename_regex",
        "extensions",
        "file_creation_date",
        "date_filter_parsed",
        "expected",
    ),
    [
        # no filters
        (False, "test.txt", None, None, 1735689600, None, True),
        # keyword matching
        (False, "TestFile.TXT", ["testfile"], None, 1735689600, None, True),
        (False, "MyReport.txt", ["report"], None, 1735689600, None, True),
        (
            False,
            "myreport.txt",
            ["MyReport"],
            None,
            1735689600,
            None,
            True,
        ),
        (False, "randomfile.txt", ["test"], None, 1735689600, None, False),
        # extension matching
        (False, "report.PDF", None, [".pdf"], 1735689600, None, True),
        (False, "summary.docx", None, [".DOCX"], 1735689600, None, True),
        (False, "logfile", None, [".txt"], 1735689600, None, False),
        # keyword + extension combination
        (False, "budget.xlsx", ["budget"], [".XLSX"], 1735689600, None, True),
        (False, "budget.xlsx", ["finance"], [".XLSX"], 1735689600, None, False),
        (False, "budget.xlsx", ["budget"], [".pdf"], 1735689600, None, False),
        (False, "data.csv", [], [], 1735689600, None, True),
        (False, "data.csv", [], [".csv"], 1735689600, None, True),
        (False, "data.csv", ["data"], [], 1735689600, None, True),
        (
            False,
            "data.csv",
            ["random"],
            [],
            1735689600,
            None,
            False,
        ),
        # exact date
        (
            False,
            "file1.txt",
            None,
            None,
            1735689600,
            pendulum.from_timestamp(1735689600).date(),
            True,
        ),
        (
            False,
            "file2.txt",
            None,
            None,
            1735689600,
            pendulum.from_timestamp(1735689600 + 86400).date(),
            False,
        ),
        # date range
        (
            False,
            "file3.txt",
            None,
            None,
            1735689600,
            (
                pendulum.from_timestamp(1735689600 - 86400).date(),
                pendulum.from_timestamp(1735689600 + 86400).date(),
            ),
            True,
        ),
        (
            False,
            "file4.txt",
            None,
            None,
            1735689600,
            (
                pendulum.from_timestamp(1735689600 + 86400).date(),
                pendulum.from_timestamp(1735689600 + 2 * 86400).date(),
            ),
            False,
        ),
        (
            False,
            "file5.txt",
            None,
            None,
            1735689600,
            (
                pendulum.from_timestamp(1735689600).date(),
                pendulum.from_timestamp(1735689600).date(),
            ),
            True,
        ),
        # combined filters
        (
            False,
            "report_2025.pdf",
            ["report"],
            [".pdf"],
            1735689600,
            pendulum.from_timestamp(1735689600).date(),
            True,
        ),
        (
            False,
            "summary.docx",
            ["summary"],
            [".docx"],
            1735689600,
            (
                pendulum.from_timestamp(1735689600 - 86400).date(),
                pendulum.from_timestamp(1735689600 + 86400).date(),
            ),
            True,
        ),
        (
            False,
            "report_2025.pdf",
            ["report"],
            [".pdf"],
            1735689600,
            (
                pendulum.from_timestamp(1735689600 + 86400).date(),
                pendulum.from_timestamp(1735689600 + 2 * 86400).date(),
            ),
            False,
        ),
        (
            False,
            "important_data.csv",
            ["important"],
            [".txt"],
            1735689600,
            (
                pendulum.from_timestamp(1735689600 - 86400).date(),
                pendulum.from_timestamp(1735689600 + 86400).date(),
            ),
            False,
        ),
        # object is directory not a file
        (True, "should_not_match.txt", None, None, 1735689600, None, False),
    ],
)
def test_is_matching_file(
    smb_instance,
    is_dir,
    name,
    filename_regex,
    extensions,
    file_creation_date,
    date_filter_parsed,
    expected,
):
    mock_entry = MagicMock()
    mock_entry.is_dir.return_value = is_dir
    mock_entry.name = name

    mock_stat = MagicMock()
    mock_stat.st_ctime = file_creation_date
    mock_entry.stat.return_value = mock_stat

    result = smb_instance._is_matching_file(
        mock_entry, filename_regex, extensions, date_filter_parsed
    )
    assert result == expected


def test_get_file_content(smb_instance, mock_smb_dir_entry_file, caplog):
    mock_entry = mock_smb_dir_entry_file
    mock_entry.name = "test_file.txt"
    mock_entry.path = f"{SERVER_PATH}/test_file.txt"
    expected_content = b"File content"

    with (
        caplog.at_level(logging.INFO),
        patch(
            "smbclient.open_file", mock_open(read_data=expected_content)
        ) as mock_file,
    ):
        result = smb_instance._get_file_content(mock_entry)

        assert isinstance(result, dict)
        assert result == {mock_entry.name: expected_content}

        mock_file.assert_called_once_with(mock_entry.path, mode="rb")

        assert f"Found: {mock_entry.path}" in caplog.text


def test_get_file_content_empty_file(smb_instance, mock_smb_dir_entry_file, caplog):
    mock_entry = mock_smb_dir_entry_file
    mock_entry.name = "empty.txt"
    mock_entry.path = f"{SERVER_PATH}/empty.txt"

    with (
        caplog.at_level(logging.INFO),
        patch("smbclient.open_file", mock_open(read_data=b"")),
    ):
        result = smb_instance._get_file_content(mock_entry)

        assert result == {mock_entry.name: b""}
        assert f"Found: {mock_entry.path}" in caplog.text


def test_save_files_locally_empty_file(smb_instance, caplog, tmp_path):
    with caplog.at_level(logging.INFO):
        smb_instance.save_files_locally({}, tmp_path)
        assert "No files to save." in caplog.text


def test_save_files_locally_single_file_save(smb_instance, tmp_path):
    file_data = {"test.txt": b"Hello, World!"}
    smb_instance.save_files_locally(file_data, tmp_path)

    saved_file = Path(tmp_path) / "test.txt"
    assert saved_file.exists()
    with saved_file.open("rb") as f:
        assert f.read() == b"Hello, World!"


def test_save_files_locally_nested_path_file_save(smb_instance, tmp_path):
    file_data = {"nested/path/test.txt": b"Nested file"}
    smb_instance.save_files_locally(file_data, tmp_path)

    saved_file = Path(tmp_path) / "test.txt"
    assert saved_file.exists()
    with saved_file.open("rb") as f:
        assert f.read() == b"Nested file"


def test_save_files_locally_multiple_files_save(smb_instance, tmp_path):
    file_data = {"test1.txt": b"File 1", "test2.txt": b"File 2", "test3.txt": b"File 3"}
    smb_instance.save_files_locally(file_data, tmp_path)

    for filename, content in file_data.items():
        saved_file = Path(tmp_path) / filename
        assert saved_file.exists(), f"{filename} should exist"
        with saved_file.open("rb") as f:
            assert f.read() == content, f"Content of {filename} should match"
