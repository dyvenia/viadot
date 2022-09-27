import os
import uuid
import pytest
from unittest import mock


from viadot.sources import AzureDataLake
from viadot.tasks import (
    AzureDataLakeCopy,
    AzureDataLakeDownload,
    AzureDataLakeList,
    AzureDataLakeToDF,
    AzureDataLakeUpload,
    AzureDataLakeRemove,
)


uuid_4 = uuid.uuid4()
uuid_4_2 = uuid.uuid4()

FILE_NAME = f"test_file_{uuid_4}.csv"
FILE_NAME_2 = f"test_file_{uuid_4}.csv"
ADLS_PATH = f"raw/supermetrics/{FILE_NAME}"
ADLS_PATH_2 = f"raw/supermetrics/{FILE_NAME_2}"

FILE_NAME_PARQUET = f"test_file_{uuid_4}.parquet"
ADLS_PATH_PARQUET = f"raw/supermetrics/{FILE_NAME_PARQUET}"

ADLS_TEST_PATHS = [
    "raw/tests/alds_test_new_fnc/2020/02/01/final_df2.csv",
    "raw/tests/alds_test_new_fnc/2020/02/01/test_new_fnc.csv",
    "raw/tests/alds_test_new_fnc/2020/02/02/final_df2.csv",
    "raw/tests/alds_test_new_fnc/2020/02/02/test_new_fnc.csv",
    "raw/tests/alds_test_new_fnc/2021/12/01/final_df2.csv",
    "raw/tests/alds_test_new_fnc/2021/12/01/test_new_fnc.csv",
    "raw/tests/alds_test_new_fnc/2022/06/21/final_df2.csv",
    "raw/tests/alds_test_new_fnc/2022/06/21/test_new_fnc.csv",
    "raw/tests/alds_test_new_fnc/2022/08/12/final_df2.csv",
    "raw/tests/alds_test_new_fnc/2022/08/12/test_new_fnc.csv",
    "raw/tests/alds_test_new_fnc/test_folder/final_df2.csv",
    "raw/tests/alds_test_new_fnc/test_folder/test_new_fnc.csv",
    "raw/tests/alds_test_new_fnc/test_folder_2/final_df2.csv",
    "raw/tests/alds_test_new_fnc/test_folder_2/test_new_fnc.csv",
]


@pytest.mark.dependency()
def test_azure_data_lake_upload(TEST_CSV_FILE_PATH):
    upload_task = AzureDataLakeUpload()
    upload_task.run(from_path=TEST_CSV_FILE_PATH, to_path=ADLS_PATH)
    file = AzureDataLake(ADLS_PATH)
    assert file.exists()


@pytest.mark.dependency(depends=["test_azure_data_lake_upload"])
def test_azure_data_lake_download():
    download_task = AzureDataLakeDownload()
    download_task.run(from_path=ADLS_PATH)
    assert os.path.exists(FILE_NAME)
    os.remove(FILE_NAME)


@pytest.mark.dependency(depends=["test_azure_data_lake_upload"])
def test_azure_data_lake_to_df():
    task = AzureDataLakeToDF()
    df = task.run(path=ADLS_PATH, sep="\t")
    assert not df.empty


@pytest.mark.dependency(
    depends=["test_azure_data_lake_upload", "test_azure_data_lake_to_df"]
)
def test_azure_data_lake_to_df_parquet(TEST_PARQUET_FILE_PATH):
    upload_task = AzureDataLakeUpload()
    upload_task.run(from_path=TEST_PARQUET_FILE_PATH, to_path=ADLS_PATH_PARQUET)

    lake_to_df_task = AzureDataLakeToDF()
    df = lake_to_df_task.run(path=ADLS_PATH_PARQUET)
    assert not df.empty


@pytest.mark.dependency(depends=["test_azure_data_lake_upload"])
def test_azure_data_lake_copy():
    copy_task = AzureDataLakeCopy()
    copy_task.run(from_path=ADLS_PATH, to_path=ADLS_PATH_2)
    file = AzureDataLake(ADLS_PATH_2)
    assert file.exists()


def test_azure_data_lake_list():
    list_task = AzureDataLakeList()
    files = list_task.run(path="raw/supermetrics")
    assert ADLS_PATH in files


def test_azure_data_lake_list_recursive():
    list_task = AzureDataLakeList()
    files = list_task.run(path="raw/tests/alds_test_new_fnc/", recursive=True)
    assert isinstance(files, list)


def test_azure_data_lake_list_paths():

    with mock.patch.object(
        AzureDataLakeList, "run", return_value=ADLS_TEST_PATHS
    ) as mock_method:

        list_task = AzureDataLakeList(path="raw/tests/alds_test_new_fnc/")
        files = list_task.run(recursive=True)
        assert files == ADLS_TEST_PATHS


@pytest.mark.dependency(depends=["test_azure_data_lake_upload"])
def test_azure_data_lake_remove():
    file = AzureDataLake(ADLS_PATH)
    assert file.exists()
    remove_task = AzureDataLakeRemove()
    remove_task.run(path=ADLS_PATH)
    assert not file.exists()
