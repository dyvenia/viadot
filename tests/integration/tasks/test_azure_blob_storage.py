from viadot.tasks import BlobFromCSV


def test_blob_from_csv(TEST_CSV_FILE_PATH, TEST_CSV_FILE_BLOB_PATH):
    blob_task = BlobFromCSV()
    result = blob_task.run(
        from_path=TEST_CSV_FILE_PATH, to_path=TEST_CSV_FILE_BLOB_PATH, overwrite=True
    )
    assert result == True
