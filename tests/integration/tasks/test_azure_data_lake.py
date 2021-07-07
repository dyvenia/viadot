import os
import uuid

from viadot.sources import AzureDataLake
from viadot.tasks import AzureDataLakeDownload, AzureDataLakeToDF, AzureDataLakeUpload

uuid_4 = uuid.uuid4()
file_name = f"test_file_{uuid_4}.csv"
adls_path = f"raw/supermetrics/{file_name}"

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
