from unittest.mock import patch

import pytest

from viadot.exceptions import CredentialError
from viadot.sources import SMB


@pytest.fixture
def valid_credentials():
    return {"username": "default@example.com", "password": "default_password"}


@pytest.fixture
def smb_instance(valid_credentials):
    return SMB(base_path="//test/path", credentials=valid_credentials)


def test_smb_initialization_with_credentials(valid_credentials):
    smb = SMB(base_path="//test/path", credentials=valid_credentials)
    assert smb.credentials["username"] == "default@example.com"
    assert smb.credentials["password"] == "default_password"  # noqa: S105


def test_smb_initialization_without_credentials():
    with pytest.raises(
        CredentialError,
        match="'username', and 'password' credentials are required.",
    ):
        SMB(base_path="//test/path")


@pytest.mark.parametrize(
    "keywords,extensions",
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
