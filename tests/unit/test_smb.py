from pathlib import Path
from unittest.mock import MagicMock, mock_open, patch

import pytest

from viadot.exceptions import CredentialError
from viadot.sources import SMB


SERVER_PATH = "//server/folder_path"


@pytest.fixture
def valid_credentials():
    return {
        "username": "default@example.com",
        "password": "default_password",  # pragma: allowlist secret
    }


@pytest.fixture
def smb_instance(valid_credentials):
    return SMB(base_path=SERVER_PATH, credentials=valid_credentials)


def test_smb_initialization_with_credentials(valid_credentials):
    smb = SMB(base_path=SERVER_PATH, credentials=valid_credentials)
    assert smb.credentials["username"] == "default@example.com"
    assert (
        smb.credentials["password"] == "default_password"  # noqa: S105, # pragma: allowlist secret
    )


def test_smb_initialization_without_credentials():
    with pytest.raises(
        CredentialError,
        match="'username', and 'password' credentials are required.",
    ):
        SMB(base_path=SERVER_PATH)


@pytest.mark.parametrize(
    ("keywords", "extensions"),
    [
        (None, None),
        (["keyword1"], None),
        (None, [".txt"]),
        (["keyword1"], [".txt"]),
    ],
)
def test_scan_and_store(smb_instance, keywords, extensions):
    with patch.object(smb_instance, "_scan_directory") as mock_scan_directory:
        smb_instance.scan_and_store(keywords=keywords, extensions=extensions)
        mock_scan_directory.assert_called_once_with(
            smb_instance.base_path, keywords, extensions
        )


def test_scan_directory(smb_instance):
    with (
        patch.object(smb_instance, "_get_directory_entries") as mock_get_entries,
        patch.object(smb_instance, "_handle_directory_entry") as mock_handle_entry,
    ):
        mock_get_entries.return_value = ["file1", "file2"]

        smb_instance._scan_directory(SERVER_PATH, None, None)

        mock_get_entries.assert_called_once_with(SERVER_PATH)
        assert mock_handle_entry.call_count == 2
        mock_handle_entry.assert_any_call("file1", SERVER_PATH, None, None)
        mock_handle_entry.assert_any_call("file2", SERVER_PATH, None, None)


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


def test_get_directory_entries(smb_instance):
    with patch("smbclient.scandir") as mock_scandir:
        mock_scandir.return_value = [MagicMock(), MagicMock()]
        entries = smb_instance._get_directory_entries(path=SERVER_PATH)
        assert list(entries) == mock_scandir.return_value
        mock_scandir.assert_called_once_with(SERVER_PATH)


def test_handle_directory_entry_dir(smb_instance):
    mock_entry = MagicMock()
    mock_entry.is_dir.return_value = True
    mock_entry.name = "test_dir"

    with patch.object(smb_instance, "_scan_directory") as mock_scan_directory:
        smb_instance._handle_directory_entry(
            mock_entry, SERVER_PATH, ["keyword"], [".txt"]
        )
        mock_scan_directory.assert_called_once_with(
            Path(f"{SERVER_PATH}/test_dir"), ["keyword"], [".txt"]
        )


def test_handle_directory_entry_file(smb_instance):
    mock_entry = MagicMock()
    mock_entry.is_dir.return_value = False
    mock_entry.name = "test_file.txt"

    with (
        patch.object(smb_instance, "_is_matching_file") as mock_is_matching,
        patch.object(smb_instance, "_store_matching_file") as mock_store_file,
    ):
        mock_is_matching.return_value = True
        smb_instance._handle_directory_entry(
            mock_entry, SERVER_PATH, ["test"], [".txt"]
        )

        mock_is_matching.assert_called_once_with(mock_entry, ["test"], [".txt"])
        mock_store_file.assert_called_once_with(
            file_path=Path(f"{SERVER_PATH}/test_file.txt")
        )


@pytest.mark.parametrize(
    ("is_file", "name", "keywords", "extensions", "expected"),
    [
        (True, "test.txt", None, None, True),
        (True, "test.txt", ["test"], None, True),
        (True, "test.txt", ["other"], None, False),
        (True, "test.txt", None, [".txt"], True),
        (True, "test.txt", None, [".doc"], False),
        (True, "test.txt", ["test"], [".txt"], True),
        (True, "test.txt", ["other"], [".doc"], False),
        (False, "test.txt", None, None, False),
    ],
)
def test_is_matching_file(smb_instance, is_file, name, keywords, extensions, expected):
    mock_entry = MagicMock()
    mock_entry.is_file.return_value = is_file
    mock_entry.name = name

    result = smb_instance._is_matching_file(mock_entry, keywords, extensions)
    assert result == expected


def test_store_matching_file(smb_instance):
    with (
        patch.object(smb_instance, "_fetch_file_content") as mock_fetch,
        patch.object(smb_instance, "logger") as mock_logger,
    ):
        mock_fetch.return_value = b"file content"
        smb_instance._store_matching_file(file_path=f"{SERVER_PATH}/file.txt")
        mock_logger.info.assert_called_once_with(f"Found: {SERVER_PATH}/file.txt")
        mock_fetch.assert_called_once_with(f"{SERVER_PATH}/file.txt")
        assert smb_instance.found_files[f"{SERVER_PATH}/file.txt"] == b"file content"


def test_fetch_file_content(smb_instance):
    mock_file_content = b"file content"
    mock_file = mock_open(read_data=mock_file_content)

    with patch("smbclient.open_file", mock_file) as mock_open_file:
        content = smb_instance._fetch_file_content(f"{SERVER_PATH}/file.txt")

        mock_open_file.assert_called_once_with(f"{SERVER_PATH}/file.txt", mode="rb")
        assert content == mock_file_content


def test_save_stored_files(smb_instance, tmp_path):
    smb_instance.found_files = {
        "/remote/path/file1.txt": b"content1",
        "/remote/path/file2.txt": b"content2",
    }

    with patch.object(smb_instance, "logger") as mock_logger:
        smb_instance.save_stored_files(str(tmp_path))

        assert (tmp_path / "file1.txt").read_bytes() == b"content1"
        assert (tmp_path / "file2.txt").read_bytes() == b"content2"

        mock_logger.info.assert_any_call(f"Saved: {tmp_path}/file1.txt")
        mock_logger.info.assert_any_call(f"Saved: {tmp_path}/file2.txt")


def test_save_stored_files_no_files(smb_instance, tmp_path):
    smb_instance.found_files = {}

    with patch.object(smb_instance, "logger") as mock_logger:
        smb_instance.save_stored_files(str(tmp_path))
        mock_logger.info.assert_called_once_with("No files to save.")


def test_save_stored_files_error(smb_instance, tmp_path):
    smb_instance.found_files = {"/remote/path/file1.txt": b"content1"}

    with (
        patch.object(smb_instance, "logger") as mock_logger,
        patch("pathlib.Path.open", side_effect=Exception("Test error")),
    ):
        smb_instance.save_stored_files(str(tmp_path))
        mock_logger.exception.assert_called_once_with(
            f"Failed to save {tmp_path}/file1.txt: Test error"
        )
