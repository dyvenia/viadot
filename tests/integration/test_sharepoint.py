import pytest
import os
import pathlib
import pandas as pd
from typing import List

from viadot.sources import Sharepoint
from viadot.flows import SharepointToADLS as s_flow
from viadot.config import local_config
from viadot.task_utils import df_get_data_types_task

s = Sharepoint()

FILE_NAME = "EUL Data.xlsm"
s.download_file(download_to_path=FILE_NAME)
DF = pd.read_excel(FILE_NAME, sheet_name=0)


def test_credentials():
    credentials = {"site": "tenant.sharepoint.com", "username": "User"}
    s = Sharepoint(credentials=credentials)
    with pytest.raises(ValueError, match="Missing credentials."):
        s.get_connection()


def test_connection():
    credentials = local_config.get("SHAREPOINT")
    site = f'https://{credentials["site"]}'
    conn = s.get_connection()
    response = conn.get(site)
    assert response.status_code == 200


def test_file_extension():
    file_ext = [".xlsm", ".xlsx"]
    assert pathlib.Path(s.download_from_path).suffix in file_ext


def test_file_name():
    assert os.path.basename(s.download_from_path) == FILE_NAME


def test_file_download():
    s.download_file(download_to_path=FILE_NAME)
    files = []
    for file in os.listdir():
        if os.path.isfile(os.path.join(file)):
            files.append(file)
    assert FILE_NAME in files
    os.remove(FILE_NAME)


def test_file_to_df():
    df_test = pd.DataFrame(data={"col1": [1, 2]})
    assert type(DF) == type(df_test)


def test_get_data_types():
    dtypes_map = df_get_data_types_task.run(DF)
    dtypes = [v for k, v in dtypes_map.items()]
    assert "String" in dtypes
