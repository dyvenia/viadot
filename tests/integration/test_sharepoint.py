import os

import pandas as pd
import pytest
from prefect.tasks.secrets import PrefectSecret

from viadot.config import local_config
from viadot.exceptions import CredentialError
from viadot.sources import Sharepoint
from viadot.task_utils import df_get_data_types_task
from viadot.tasks.sharepoint import SharepointToDF


def get_url() -> str:
    """
    Function to get file URL.

    Returns:
        str: File URL.
    """
    return local_config["SHAREPOINT"].get("url")


@pytest.fixture(scope="session")
def sharepoint():
    """
    Fixture for creating a Sharepoint class instance.
    The class instance can be used within a test functions to interact with Sharepoint.
    """
    s = Sharepoint()
    yield s


@pytest.fixture(scope="session")
def file_name(sharepoint):
    """
    A function built to get the path to a file.

    Args:
        sharepoint (Sharepoint): Sharepoint class instance.
    """
    path = "Questionnaires.xlsx"
    sharepoint.download_file(download_to_path=path, download_from_path=get_url())
    yield path
    os.remove(path)


def test_credentials_not_found():
    """
    Testing if a VauleError is thrown when none of credentials are given.

    Args:
        sharepoint (Sharepoint): Sharepoint class instance.
    """
    none_credentials = None
    with pytest.raises(CredentialError, match=r"Credentials not found."):
        Sharepoint(credentials=none_credentials)


def test_get_connection_credentials():
    """
    Testing if a CredentialError is thrown when credentials doesn't contain required keys.

    Args:
        sharepoint (Sharepoint): Sharepoint class instance.
    """
    credentials = {"site": "tenant.sharepoint.com", "username": "User"}
    s = Sharepoint(credentials=credentials)
    with pytest.raises(CredentialError, match="Missing credentials."):
        s.get_connection()


def test_connection(sharepoint):
    """
    Testing if connection is succesfull with given credentials.

    Args:
        sharepoint (Sharepoint): Sharepoint class instance.
    """
    credentials = local_config.get("SHAREPOINT")
    site = f'https://{credentials["site"]}'
    conn = sharepoint.get_connection()
    response = conn.get(site)
    assert response.status_code == 200


def test_sharepoint_to_df_task():
    """Testing if result of `SharepointToDF` is a Data Frame."""
    task = SharepointToDF()
    credentials_secret = PrefectSecret("SHAREPOINT_KV").run()
    res = task.run(
        credentials_secret=credentials_secret,
        sheet_number=0,
        path_to_file="Questionnaires.xlsx",
        url_to_file=get_url(),
    )
    assert isinstance(res, pd.DataFrame)
    os.remove("Questionnaires.xlsx")


def test_download_file_missing_patameters(sharepoint):
    """
    Testing if a VauleError is thrown when none of the parameters are given.

    Args:
        sharepoint (Sharepoint): Sharepoint class instance.
    """
    with pytest.raises(ValueError, match=r"Missing required parameter"):
        sharepoint.download_file(download_to_path=None, download_from_path=None)


def test_file_download(file_name):
    """
    Testing if file is downloaded.

    Args:
        file_name (str): File name.
    """
    files = []
    for file in os.listdir():
        if os.path.isfile(os.path.join(file)):
            files.append(file)
    assert file_name in files


def test_autopopulating_download_from(file_name):
    """
    Testing if file name is correct.

    Args:
        file_name (str): File name.
    """
    assert os.path.basename(get_url()) == file_name


def test_file_extension():
    """Testing if file has correct extension."""
    file_ext = (".xlsm", ".xlsx")
    assert get_url().endswith(file_ext)


def test_file_to_df(file_name):
    """
    Testing if downloaded file contains data and if first sheet can be build as a Data frame.

    Args:
        file_name (str): File name.
    """
    df = pd.read_excel(file_name, sheet_name=0)
    df_test = pd.DataFrame(data={"col1": [1, 2]})
    assert type(df) == type(df_test)


def test_get_data_types(file_name):
    """
    Testing if downloaded file contains data and columns have `String` type.

    Args:
        file_name (str): File name.
    """
    df = pd.read_excel(file_name, sheet_name=0)
    dtypes_map = df_get_data_types_task.run(df)
    dtypes = dtypes_map.values()

    assert "String" in dtypes
