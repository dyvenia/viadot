import pandas as pd

from viadot.utils import skip_test_on_missing_extra


try:
    from viadot.sources import AzureDataLake
except ImportError:
    skip_test_on_missing_extra(source_name="AzureDataLake", extra="azure")


def test_upload_csv(TEST_CSV_FILE_PATH, TEST_ADLS_FILE_PATH_CSV):
    lake = AzureDataLake(config_key="adls_test")

    assert not lake.exists(TEST_ADLS_FILE_PATH_CSV)

    lake.upload(
        from_path=TEST_CSV_FILE_PATH, to_path=TEST_ADLS_FILE_PATH_CSV, overwrite=True
    )

    assert lake.exists(TEST_ADLS_FILE_PATH_CSV)

    lake.rm(TEST_ADLS_FILE_PATH_CSV)


def test_upload_parquet(TEST_PARQUET_FILE_PATH, TEST_ADLS_FILE_PATH_PARQUET):
    lake = AzureDataLake(config_key="adls_test")

    assert not lake.exists(TEST_ADLS_FILE_PATH_PARQUET)

    lake.upload(
        from_path=TEST_PARQUET_FILE_PATH,
        to_path=TEST_ADLS_FILE_PATH_PARQUET,
        overwrite=True,
    )

    assert lake.exists(TEST_ADLS_FILE_PATH_PARQUET)

    lake.rm(TEST_ADLS_FILE_PATH_PARQUET)


def test_from_df_csv(TEST_ADLS_FILE_PATH_CSV):
    lake = AzureDataLake(config_key="adls_test")

    assert not lake.exists(TEST_ADLS_FILE_PATH_CSV)

    df = pd.DataFrame({"a": [1, 2], "b": [3, 4]})
    lake.from_df(
        df=df,
        path=TEST_ADLS_FILE_PATH_CSV,
    )

    assert lake.exists(TEST_ADLS_FILE_PATH_CSV)

    lake.rm(TEST_ADLS_FILE_PATH_CSV)


def test_from_df_parquet(TEST_ADLS_FILE_PATH_PARQUET):
    lake = AzureDataLake(config_key="adls_test")

    assert not lake.exists(TEST_ADLS_FILE_PATH_PARQUET)

    df = pd.DataFrame({"a": [1, 2], "b": [3, 4]})
    lake.from_df(
        df=df,
        path=TEST_ADLS_FILE_PATH_PARQUET,
    )

    assert lake.exists(TEST_ADLS_FILE_PATH_PARQUET)

    lake.rm(TEST_ADLS_FILE_PATH_PARQUET)
