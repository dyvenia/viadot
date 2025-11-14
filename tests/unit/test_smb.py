import io
import logging
from pathlib import Path
from unittest.mock import MagicMock, mock_open, patch
import zipfile

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
    with patch("viadot.sources.smb.smbclient.register_session") as mock_register:
        mock_register.return_value = None
        return SMB(base_paths=[SERVER_PATH], credentials=valid_credentials)


@pytest.fixture
def mock_smb_dir_entry_file():
    mock = MagicMock()
    mock.name = "test_file.txt"
    mock.paths = f"{SERVER_PATH}/test_file.txt"
    mock.is_dir.return_value = False
    mock.is_file.return_value = True
    mock.stat.return_value.st_ctime = pendulum.now().timestamp()
    return mock


@pytest.fixture
def mock_smb_dir_entry_dir():
    mock = MagicMock()
    mock.name = "test_folder_path"
    mock.paths = SERVER_PATH
    mock.is_dir.return_value = True
    mock.is_file.return_value = False
    return mock


@pytest.fixture
def sample_zip_bytes():
    """Create an in-memory ZIP file for testing."""
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr("file1.txt", b"Hello World")
        zf.writestr("file2.csv", b"1,2,3")
        zf.writestr("folder/file3.txt", b"Nested file")
    buf.seek(0)
    return buf


def test_smb_initialization_with_credentials(valid_credentials):
    with patch("viadot.sources.smb.smbclient.register_session") as mock_register:
        mock_register.return_value = None
        smb = SMB(base_paths=[SERVER_PATH], credentials=valid_credentials)
    assert smb.credentials["username"] == "default@example.com"
    assert smb.credentials["password"] == SecretStr("default_password")


def test_smb_initialization_without_credentials():
    with pytest.raises(
        CredentialError,
        match="`username`, and `password` credentials are required.",
    ):
        SMB(base_paths=[SERVER_PATH])


@pytest.mark.parametrize(
    (
        "filename_regex",
        "extensions",
        "date_filter",
    ),
    [
        ([None], None, "<<pendulum.yesterday().date()>>"),
        (["keyword1"], None, "<<pendulum.yesterday().date()>>"),
        ([None], [".txt"], "<<pendulum.yesterday().date()>>"),
        (["keyword1"], [".txt"], "<<pendulum.yesterday().date()>>"),
    ],
)
def test_scan_and_store(
    smb_instance,
    filename_regex,
    extensions,
    date_filter,
):
    with (
        patch.object(smb_instance, "_scan_directories") as mock_scan_directories,
        patch("viadot.sources.smb.parse_dates") as mock_parse_dates,
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

        mock_scan_directories.assert_called_once_with(
            paths=smb_instance.base_paths,
            filename_regex=filename_regex,
            extensions=extensions,
            date_filter_parsed=mock_date_result,
            prefix_levels_to_add=0,
            zip_inner_file_regexes=None,
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
            mock_smb_dir_entry_file.paths: mock_file_content
        }
        mock_is_matching.return_value = True

        result_dict, result_list = smb_instance.scan_and_store()

        assert isinstance(result_dict, dict)
        assert isinstance(result_list, list)
        assert len(result_dict) == 1, f"Expected 1 file, got {len(result_dict)}"
        assert mock_smb_dir_entry_file.paths in result_dict
        assert result_dict[mock_smb_dir_entry_file.paths] == mock_file_content


def test_scan_directories_recursive_search(
    smb_instance, mock_smb_dir_entry_dir, mock_smb_dir_entry_file
):
    with (
        patch("smbclient.scandir") as mock_scandir,
        patch.object(smb_instance, "_get_file_content") as mock_get_content,
        patch.object(smb_instance, "_is_matching_file") as mock_is_matching,
    ):
        # Configure directory structure
        root_dir = mock_smb_dir_entry_dir
        root_dir.name = "root"
        root_dir.path = SERVER_PATH
        # root_dir.paths = [root_dir.path]
        root_dir.is_dir.return_value = True
        root_dir.is_file.return_value = False

        sub_dir = MagicMock()
        sub_dir.name = "subdir"
        sub_dir.path = f"{SERVER_PATH}/subdir"
        sub_dir.is_dir.return_value = True
        sub_dir.is_file.return_value = False

        subsub_dir = MagicMock()
        subsub_dir.name = "subsubdir"
        subsub_dir.path = f"{SERVER_PATH}/subdir/subsubdir"
        subsub_dir.is_dir.return_value = True
        subsub_dir.is_file.return_value = False

        nested_file = mock_smb_dir_entry_file
        nested_file.name = "file.txt"
        nested_file.path = f"{SERVER_PATH}/subdir/subsubdir/file.txt"
        nested_file.is_dir.return_value = False
        nested_file.is_file.return_value = True

        mock_scandir.side_effect = [
            [sub_dir],  # Initial root directory scan
            [subsub_dir],  # First subdirectory scan
            [nested_file],  # Final nested directory scan
        ]

        mock_is_matching.return_value = True
        mock_file_content = b"Recursive content"
        mock_get_content.return_value = {nested_file.paths[0]: mock_file_content}

        # Execute the scan starting at root
        res_dict, res_list = smb_instance._scan_directories(
            paths=[SERVER_PATH],
            filename_regex=None,
            extensions=None,
            date_filter_parsed=None,
        )

        # Verify scanning sequence
        assert mock_scandir.call_count == 3, "Should scan 3 levels deep"
        assert {call[0][0] for call in mock_scandir.call_args_list} == {
            SERVER_PATH,
            f"{SERVER_PATH}/subdir",
            f"{SERVER_PATH}/subdir/subsubdir",
        }

        assert isinstance(res_dict, dict)
        assert isinstance(res_list, list)
        assert (
            len(res_dict) == 1
        ), f"Expected 1 file, got {len(res_dict)}. Result: {res_dict}"
        assert nested_file.paths[0] in res_dict
        assert res_dict[nested_file.path[0]] == mock_file_content
        mock_is_matching.assert_any_call(
            file_name=nested_file.name,
            file_mod_date_parsed=pendulum.date(1970, 1, 1),
            filename_regex=None,
            extensions=None,
            date_filter_parsed=None,
        )


@patch("smbclient.scandir")
def test_empty_directory_scan(mock_scandir, smb_instance):
    """Test scanning empty directory structure."""
    mock_scandir.side_effect = lambda path: [] if path == "/empty" else []

    smb_instance._handle_matching_file = MagicMock()
    smb_instance._scan_directories(["/empty"], None, None, None)

    smb_instance._handle_matching_file.assert_not_called()
    mock_scandir.assert_called_once_with("/empty")


def test_scan_directories_error_handling(smb_instance):
    with patch.object(smb_instance, "_get_directory_entries") as mock_get_entries:
        mock_get_entries.side_effect = Exception("Test error")

        with pytest.raises(Exception, match="Test error"):
            smb_instance._scan_directories(SERVER_PATH, None, None)


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

        assert all(entry.paths.startswith(SERVER_PATH) for entry in result_entries)


@pytest.mark.parametrize(
    (
        "name",
        "filename_regex",
        "extensions",
        "file_modification_date",
        "date_filter_parsed",
        "expected",
    ),
    [
        # no filters
        ("test.txt", None, None, 1735689600, None, True),
        # keyword matching
        ("TestFile.TXT", ["testfile"], None, 1735689600, None, True),
        ("MyReport.txt", ["report"], None, 1735689600, None, True),
        ("myreport.txt", ["MyReport"], None, 1735689600, None, True),
        ("randomfile.txt", ["test"], None, 1735689600, None, False),
        # extension matching
        ("report.PDF", None, [".pdf"], 1735689600, None, True),
        ("summary.docx", None, [".DOCX"], 1735689600, None, True),
        ("logfile", None, [".txt"], 1735689600, None, False),
        # keyword + extension combination
        ("budget.xlsx", ["budget"], [".XLSX"], 1735689600, None, True),
        ("budget.xlsx", ["finance"], [".XLSX"], 1735689600, None, False),
        ("budget.xlsx", ["budget"], [".pdf"], 1735689600, None, False),
        ("data.csv", [], [], 1735689600, None, True),
        ("data.csv", [], [".csv"], 1735689600, None, True),
        ("data.csv", ["data"], [], 1735689600, None, True),
        ("data.csv", ["random"], [], 1735689600, None, False),
        # exact date
        (
            "file1.txt",
            None,
            None,
            1735689600,
            pendulum.from_timestamp(1735689600).date(),
            True,
        ),
        (
            "file2.txt",
            None,
            None,
            1735689600,
            pendulum.from_timestamp(1735689600 + 86400).date(),
            False,
        ),
        # date range
        (
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
            "report_2025.pdf",
            ["report"],
            [".pdf"],
            1735689600,
            pendulum.from_timestamp(1735689600).date(),
            True,
        ),
        (
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
    ],
)
def test_is_matching_file(
    smb_instance,
    name,
    filename_regex,
    extensions,
    file_modification_date,
    date_filter_parsed,
    expected,
):
    mock_entry = MagicMock()
    mock_entry.name = name

    mock_stat = MagicMock()
    mock_stat.st_mtime = file_modification_date
    mock_entry.stat.return_value = mock_stat

    result = smb_instance._is_matching_file(
        file_name=name,
        file_mod_date_parsed=pendulum.from_timestamp(file_modification_date).date(),
        filename_regex=filename_regex,
        extensions=extensions,
        date_filter_parsed=date_filter_parsed,
    )
    assert result == expected


def test_get_file_content(smb_instance, mock_smb_dir_entry_file, caplog):
    mock_entry = mock_smb_dir_entry_file
    mock_entry.name = "test_file.txt"
    mock_entry.path = f"{SERVER_PATH}/test_file.txt"
    mock_entry.paths = [mock_entry.path]
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

        mock_file.assert_called_once_with(mock_entry.paths[0], mode="rb")

        assert f"Found: {mock_entry.paths[0]}" in caplog.text


def test_get_file_content_empty_file(smb_instance, mock_smb_dir_entry_file, caplog):
    mock_entry = mock_smb_dir_entry_file
    mock_entry.name = "empty.txt"
    mock_entry.path = f"{SERVER_PATH}/empty.txt"
    mock_entry.paths = [mock_entry.path]

    with (
        caplog.at_level(logging.INFO),
        patch("smbclient.open_file", mock_open(read_data=b"")),
    ):
        result = smb_instance._get_file_content(mock_entry)

        assert isinstance(result, dict)
        assert result == {mock_entry.name: b""}
        assert f"Found: {mock_entry.paths[0]}" in caplog.text


def test_get_file_content_zip_all_files(
    smb_instance, mock_smb_dir_entry_file, sample_zip_bytes
):
    """Test unpacking ZIP with no filter (all files extracted)."""
    mock_entry = mock_smb_dir_entry_file
    mock_entry.name = "file.zip"
    mock_entry.paths = f"{SERVER_PATH}/file.zip"

    with patch("smbclient.open_file", return_value=sample_zip_bytes):
        smb_instance._build_prefix_from_path = lambda path, levels: ""  # noqa: ARG005
        smb_instance._add_prefix_to_filename = lambda name, prefix: name  # noqa: ARG005
        smb_instance._matches_any_regex = (
            lambda text, patterns: True  # noqa: ARG005
        )  # match all

        contents = smb_instance._get_file_content(
            mock_entry, zip_inner_file_regexes="file"
        )

        assert len(contents) == 3
        assert contents["file1.txt"] == b"Hello World"
        assert contents["file2.csv"] == b"1,2,3"
        assert contents["file3.txt"] == b"Nested file"


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


@pytest.mark.parametrize(
    ("file_path", "levels", "expected_prefix"),
    [
        ("/root/DATA/12345/file.txt", 0, ""),
        ("/root/DATA/12345/file.txt", 1, "12345"),
        ("/root/DATA/12345/subdir/file.txt", 2, "12345_subdir"),
        ("//root/DATA/file.txt", 3, "root_DATA"),
        ("/a/b/c/d/file.txt", 4, "a_b_c_d"),
        ("file.txt", 1, ""),
        ("\\\\root\\DATA\\1234\\file.txt", 2, "DATA_1234"),
    ],
)
def test_build_prefix_from_path(smb_instance, file_path, levels, expected_prefix):
    result = smb_instance._build_prefix_from_path(file_path=file_path, levels=levels)
    assert result == expected_prefix


@pytest.mark.parametrize(
    ("filename", "prefix", "expected"),
    [
        ("file.txt", "", "file.txt"),
        ("file.txt", "12345", "12345_file.txt"),
        ("data.csv", "a_b", "a_b_data.csv"),
    ],
)
def test_add_prefix_to_filename(smb_instance, filename, prefix, expected):
    result = smb_instance._add_prefix_to_filename(filename=filename, prefix=prefix)
    assert result == expected


def test_integration_prefix_and_add_filename(smb_instance):
    """Integration-style test that combines both functions."""
    file_path = "DATA/12345/sub1/sub2/sample.csv"
    prefix = smb_instance._build_prefix_from_path(file_path=file_path, levels=2)
    result = smb_instance._add_prefix_to_filename(
        filename=Path(file_path).name, prefix=prefix
    )
    assert result == "sub1_sub2_sample.csv"


def test_negative_prefix_levels(smb_instance):
    path = "/some/path/file.log"
    prefix = smb_instance._build_prefix_from_path(file_path=path, levels=-5)
    result = smb_instance._add_prefix_to_filename(
        filename=Path(path).name, prefix=prefix
    )

    assert result == "file.log"


def test_matches_any_regex_true(smb_instance):
    zip_member_name = "file1.txt"
    zip_inner_file_regexes_1 = None
    zip_inner_file_regexes_2 = r"^file1\.txt$"
    zip_inner_file_regexes_3 = [r"^file1", r"txt$"]

    result1 = smb_instance._matches_any_regex(
        text=zip_member_name, patterns=zip_inner_file_regexes_1
    )
    result2 = smb_instance._matches_any_regex(
        text=zip_member_name, patterns=zip_inner_file_regexes_2
    )
    result3 = smb_instance._matches_any_regex(
        text=zip_member_name, patterns=zip_inner_file_regexes_3
    )

    assert result1 is True
    assert result2 is True
    assert result3 is True


def test_matches_any_regex_false(smb_instance):
    zip_member_name = "file1.txt"
    zip_inner_file_regexes = r"^file2\.zip$"

    result = smb_instance._matches_any_regex(
        text=zip_member_name, patterns=zip_inner_file_regexes
    )

    assert result is False
