from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from viadot.exceptions import CredentialError
from viadot.sources import SMB


SERVER_PATH = "//server/folder_path"


@pytest.fixture
def valid_credentials():
    return {"username": "default@example.com", "password": "default_password"}


@pytest.fixture
def smb_instance(valid_credentials):
    return SMB(base_path=SERVER_PATH, credentials=valid_credentials)


def test_smb_initialization_with_credentials(valid_credentials):
    smb = SMB(base_path=SERVER_PATH, credentials=valid_credentials)
    assert smb.credentials["username"] == "default@example.com"
    assert smb.credentials["password"] == "default_password"  # noqa: S105


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

        smb_instance._scan_directory("/test/path", None, None)

        mock_logger.exception.assert_called_once_with(
            "Error scanning or downloading from /test/path: Test error"
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
            mock_entry, SERVER_PATH, ["keyword"], [".txt"]
        )
        mock_is_matching.assert_called_once_with(mock_entry, ["keyword"], [".txt"])
        mock_store_file.assert_called_once_with(
            file_path=Path(f"{SERVER_PATH}/test_file.txt")
        )
