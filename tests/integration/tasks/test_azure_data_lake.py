import os
import uuid

from viadot.sources import AzureDataLake
from viadot.tasks import (
    AzureDataLakeDownload,
    AzureDataLakeToDF,
    AzureDataLakeUpload,
    AzureDataLakeCopy,
    AzureDataLakeList,
)

uuid_4 = uuid.uuid4()
uuid_4_2 = uuid.uuid4()

file_name = f"test_file_{uuid_4}.csv"
file_name_2 = f"test_file_{uuid_4}.csv"
adls_path = f"raw/supermetrics/{file_name}"
adls_path_2 = f"raw/supermetrics/{file_name_2}"

file_name_parquet = f"test_file_{uuid_4}.parquet"
adls_path_parquet = f"raw/supermetrics/{file_name_parquet}"

# TODO: add pytest-depends as download tests depend on the upload
# and can't be ran separately


def test_azure_data_lake_upload(TEST_CSV_FILE_PATH):
    upload_task = AzureDataLakeUpload()
    upload_task.run(from_path=TEST_CSV_FILE_PATH, to_path=adls_path)
    file = AzureDataLake(adls_path)
    assert file.exists()


def test_azure_data_lake_download():
    download_task = AzureDataLakeDownload()
    download_task.run(from_path=adls_path)
    assert os.path.exists(file_name)
    os.remove(file_name)


def test_azure_data_lake_to_df():
    task = AzureDataLakeToDF()
    df = task.run(path=adls_path, sep="\t")
    assert not df.empty


def test_azure_data_lake_to_df_parquet(TEST_PARQUET_FILE_PATH):
    upload_task = AzureDataLakeUpload()
    upload_task.run(from_path=TEST_PARQUET_FILE_PATH, to_path=adls_path_parquet)

    lake_to_df_task = AzureDataLakeToDF()
    df = lake_to_df_task.run(path=adls_path_parquet)
    assert not df.empty


def test_azure_data_lake_copy():
    copy_task = AzureDataLakeCopy()
    copy_task.run(from_path=adls_path, to_path=adls_path_2)
    file = AzureDataLake(adls_path_2)
    assert file.exists()


def test_azure_data_lake_list():
    list_task = AzureDataLakeList()
    files = list_task.run(path="raw/supermetrics")
    assert adls_path in files
