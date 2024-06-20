import pytest

try:
    from viadot.sources import MinIO

    _minio_installed = True
except ImportError:
    _minio_installed = False

if not _minio_installed:
    pytest.skip("MinIO source not installed", allow_module_level=True)


TEST_BUCKET = "spark"
TEST_SCHEMA = "test_schema"
TEST_SCHEMA_URL = f"s3a://{TEST_BUCKET}/{TEST_SCHEMA}"
TEST_SCHEMA_PATH = TEST_SCHEMA
TEST_TABLE = "test_table"
TEST_TABLE_URL = f"{TEST_SCHEMA_URL}/{TEST_TABLE}"
TEST_TABLE_PATH = f"{TEST_SCHEMA_PATH}/{TEST_TABLE}"
TEST_TABLE_FILE_PATH = f"{TEST_TABLE_PATH}/{TEST_TABLE}.parquet"


@pytest.fixture(scope="session")
def minio(minio_config_key):
    minio = MinIO(config_key=minio_config_key)
    minio.rm(TEST_TABLE_FILE_PATH)

    yield minio


def test_check_connection(minio):
    try:
        minio.check_connection()
    except Exception as e:
        assert False, f"Exception:\n{e}"


def test_from_df(minio, DF):
    # Assumptions.
    file_exists = minio._check_if_file_exists(TEST_TABLE_FILE_PATH)
    assert not file_exists

    # Test.
    minio.from_df(DF, schema_name=TEST_SCHEMA, table_name=TEST_TABLE)

    file_exists = minio._check_if_file_exists(TEST_TABLE_FILE_PATH)
    assert file_exists

    # Cleanup.
    minio.rm(TEST_TABLE_FILE_PATH)


def test_from_df_path(minio, DF):
    """Test that the `path` parameter in `from_df()` works."""
    # Assumptions.
    file_exists = minio._check_if_file_exists(TEST_TABLE_FILE_PATH)
    assert not file_exists

    # Test.
    minio.from_df(DF, path=TEST_TABLE_FILE_PATH)

    file_exists = minio._check_if_file_exists(TEST_TABLE_FILE_PATH)
    assert file_exists

    # Cleanup.
    minio.rm(TEST_TABLE_FILE_PATH)


def test_ls(minio, DF):
    # Assumptions.
    files = list(minio.ls(TEST_TABLE_PATH + "/"))
    assert TEST_TABLE_FILE_PATH not in files

    # Test.
    minio.from_df(DF, path=TEST_TABLE_FILE_PATH)
    files = list(minio.ls(TEST_TABLE_PATH + "/"))
    assert TEST_TABLE_FILE_PATH in files

    # Cleanup.
    minio.rm(TEST_TABLE_FILE_PATH)
