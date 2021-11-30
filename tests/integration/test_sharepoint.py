import pytest
import os
import pathlib
import pandas as pd
from viadot.exceptions import CredentialError

from viadot.sources import Sharepoint
from viadot.config import local_config
from viadot.task_utils import df_get_data_types_task


@pytest.fixture(scope="session")
def sharepoint():
    s = Sharepoint()
    yield s


@pytest.fixture(scope="session")
def FILE_NAME(sharepoint):
    path = "EUL Data.xlsm"
    sharepoint.download_file(download_to_path=path)
    yield path
    os.remove(path)


def test_credentials():
    credentials = {"site": "tenant.sharepoint.com", "username": "User"}
    s = Sharepoint(credentials=credentials)
    with pytest.raises(CredentialError, match="Missing credentials."):
        s.get_connection()


def test_connection(sharepoint):
    credentials = local_config.get("SHAREPOINT")
    site = f'https://{credentials["site"]}'
    conn = sharepoint.get_connection()
    response = conn.get(site)
    assert response.status_code == 200


def test_file_download(FILE_NAME):
    files = []
    for file in os.listdir():
        if os.path.isfile(os.path.join(file)):
            files.append(file)
    assert FILE_NAME in files


def test_autopopulating_download_from(FILE_NAME):
    assert os.path.basename(sharepoint.download_from_path) == FILE_NAME


def test_file_extension(sharepoint):
    file_ext = [".xlsm", ".xlsx"]
    assert pathlib.Path(sharepoint.download_from_path).suffix in file_ext


def test_file_to_df(FILE_NAME):
    df = pd.read_excel(FILE_NAME, sheet_name=0)
    df_test = pd.DataFrame(data={"col1": [1, 2]})
    assert type(df) == type(df_test)


def test_get_data_types(FILE_NAME):
    df = pd.read_excel(FILE_NAME, sheet_name=0)
    dtypes_map = df_get_data_types_task.run(df)
    dtypes = [v for k, v in dtypes_map.items()]
    assert "String" in dtypes
