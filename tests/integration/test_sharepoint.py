import os
import pandas as pd
import pytest
from prefect.tasks.secrets import PrefectSecret

from viadot.config import local_config
from viadot.exceptions import CredentialError

from viadot.sources import Sharepoint
from viadot.task_utils import df_get_data_types_task
from viadot.tasks.sharepoint import SharepointToDF


def get_url():
    return local_config["SHAREPOINT"].get("url")


@pytest.fixture(scope="session")
def sharepoint():
    s = Sharepoint()
    yield s


@pytest.fixture(scope="session")
def FILE_NAME(sharepoint):
    path = "Questionnaires.xlsx"
    sharepoint.download_file(download_to_path=path, download_from_path=get_url())
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


def test_sharepoint_to_df_task():
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


def test_file_download(FILE_NAME):
    files = []
    for file in os.listdir():
        if os.path.isfile(os.path.join(file)):
            files.append(file)
    assert FILE_NAME in files


def test_autopopulating_download_from(FILE_NAME):
    assert os.path.basename(get_url()) == FILE_NAME


def test_file_extension(sharepoint):
    file_ext = (".xlsm", ".xlsx")
    assert get_url().endswith(file_ext)


def test_file_to_df(FILE_NAME):
    df = pd.read_excel(FILE_NAME, sheet_name=0)
    df_test = pd.DataFrame(data={"col1": [1, 2]})
    assert type(df) == type(df_test)


def test_get_data_types(FILE_NAME):
    df = pd.read_excel(FILE_NAME, sheet_name=0)
    dtypes_map = df_get_data_types_task.run(df)
    dtypes = dtypes_map.values()

    assert "String" in dtypes
