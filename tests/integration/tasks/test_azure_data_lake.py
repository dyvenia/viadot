import os
import uuid
import pytest

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


@pytest.mark.dependency(depends=["test_azure_data_lake_upload"])
def test_azure_data_lake_remove():
    file = AzureDataLake(ADLS_PATH)
    assert file.exists()
    remove_task = AzureDataLakeRemove()
    remove_task.run(path=ADLS_PATH)
    assert not file.exists()
