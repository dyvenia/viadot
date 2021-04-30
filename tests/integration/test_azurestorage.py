from viadot.sources.azure_blob_storage import AzureBlobStorage


def test_to_storage(TEST_CSV_FILE_PATH, TEST_CSV_FILE_BLOB_PATH):
    azstorage = AzureBlobStorage()
    azstorage.to_storage(from_path=TEST_CSV_FILE_PATH, to_path=TEST_CSV_FILE_BLOB_PATH)
