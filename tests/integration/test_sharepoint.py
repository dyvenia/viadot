import os
from pathlib import Path

import pytest

from viadot.exceptions import CredentialError
from viadot.sources import Sharepoint


def test_is_configured():
    s = Sharepoint(
        credentials={
            "site": "test_site",
            "username": "test_user",
            "password": "test_password",
        },
    )
    assert s


def test_is_configured_throws_credential_error():
    with pytest.raises(CredentialError):
        _ = Sharepoint(
            credentials={
                "site": None,
                "username": "test_user",
                "password": "test_password",
            },
        )
    with pytest.raises(CredentialError):
        _ = Sharepoint(
            credentials={
                "site": "test_site",
                "username": None,
                "password": "test_password",
            },
        )
    with pytest.raises(CredentialError):
        _ = Sharepoint(
            credentials={
                "site": "test_site",
                "username": "test_user",
                "password": None,
            },
        )


def test_to_df(sharepoint_url, sharepoint_config_key):
    s = Sharepoint(config_key=sharepoint_config_key)
    df = s.to_df(sharepoint_url, usecols="A")
    assert not df.empty


def test_download_file(sharepoint_url, sharepoint_config_key):
    s = Sharepoint(config_key=sharepoint_config_key)

    file = "sharepoint_test" + "." + sharepoint_url.split(".")[-1]
    s.download_file(url=sharepoint_url, to_path=file)

    assert file in os.listdir()

    Path(file).unlink()
