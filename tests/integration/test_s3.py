import contextlib
import os

import pandas as pd
import pytest

from viadot.utils import skip_test_on_missing_extra


try:
    from viadot.sources import S3
except ImportError:
    skip_test_on_missing_extra(source_name="S3", extra="aws")

from pathlib import Path


SOURCE_DATA = [
    {
        "Name": "Scott-Merritt",
        "FirstName": "Melody",
        "LastName": "Cook",
        "ContactEmail": "Melody.Cook@ScottMerritt.com",
        "MailingCity": "Elizabethfurt",
    },
    {
        "Name": "Mann-Warren",
        "FirstName": "Wayne",
        "LastName": "Morrison",
        "ContactEmail": "Wayne.Morrison@MannWarren.com",
        "MailingCity": "Kathrynmouth",
    },
]
TEST_DF = pd.DataFrame(SOURCE_DATA)

S3_BUCKET = os.environ.get("VIADOT_S3_BUCKET")
TEST_SCHEMA = "test_schema"
TEST_TABLE = "test_table"
TEST_TABLE_PATH = f"s3://{S3_BUCKET}/viadot/{TEST_SCHEMA}/{TEST_TABLE}"
TEST_TABLE_PATH_PARQUET = str(Path(TEST_TABLE_PATH) / f"{TEST_TABLE}.parquet")
TEST_TABLE_PATH_CSV = str(Path(TEST_TABLE_PATH) / f"{TEST_TABLE}.csv")


@pytest.fixture(scope="session")
def s3(s3_config_key):
    s3 = S3(config_key=s3_config_key)

    yield s3

    # Remove the s3 table folder.
    with contextlib.suppress(Exception):
        s3.rm(path=TEST_TABLE_PATH)


def test_from_df(s3):
    # Assumptions.
    exists = s3.exists(path=TEST_TABLE_PATH)
    assert exists is False

    # Test.
    s3.from_df(df=TEST_DF, path=TEST_TABLE_PATH)
    result = s3.exists(path=TEST_TABLE_PATH)
    assert result is True

    # Cleanup.
    s3.rm(path=TEST_TABLE_PATH)


def test_from_df_max_rows(s3):
    rows_per_file = 1
    size_df = len(TEST_DF)
    amount_of_expected_files = int(size_df / rows_per_file)

    s3.from_df(
        df=TEST_DF,
        path=TEST_TABLE_PATH,
        max_rows_by_file=rows_per_file,
    )

    counter_files = 0

    for i in range(0, amount_of_expected_files, 1):
        if s3.exists(path=f"{TEST_TABLE_PATH}_{i}"):
            counter_files = counter_files + 1
            s3.rm(path=[f"{TEST_TABLE_PATH}_{i}"])

    assert (counter_files == amount_of_expected_files) is True

    # Cleanup.
    s3.rm(path=TEST_TABLE_PATH)


def test_to_df(s3):
    # Assumptions.
    exists = s3.exists(path=TEST_TABLE_PATH_PARQUET)
    assert exists is False

    # Test.
    s3.from_df(df=TEST_DF, path=TEST_TABLE_PATH_PARQUET)
    result = s3.to_df(paths=[TEST_TABLE_PATH_PARQUET])
    assert len(result) == len(TEST_DF)

    # Cleanup.
    s3.rm(path=TEST_TABLE_PATH)


def test_to_df_chunk_size(s3):
    # Assumptions.
    exists = s3.exists(path=TEST_TABLE_PATH_PARQUET)
    assert exists is False

    # Test.
    s3.from_df(df=TEST_DF, path=TEST_TABLE_PATH_PARQUET)

    dfs = s3.to_df(
        paths=[TEST_TABLE_PATH_PARQUET],
        chunk_size=1,
    )

    result = pd.concat(dfs, ignore_index=True)
    assert len(result) == len(TEST_DF)

    # Cleanup.
    s3.rm(path=TEST_TABLE_PATH)


def test_upload(s3, TEST_CSV_FILE_PATH):
    # Assumptions.
    exists = s3.exists(path=TEST_TABLE_PATH_CSV)
    assert exists is False

    # Test.
    s3.upload(
        from_path=TEST_CSV_FILE_PATH,
        to_path=TEST_TABLE_PATH_CSV,
    )

    exists = s3.exists(path=TEST_TABLE_PATH_CSV)
    assert exists is True

    # Cleanup.
    s3.rm(path=TEST_TABLE_PATH)


def test_download(s3, TEST_CSV_FILE_PATH):
    # Assumptions.
    assert not Path("test.csv").exists()

    s3.upload(
        from_path=TEST_CSV_FILE_PATH,
        to_path=TEST_TABLE_PATH_CSV,
    )
    exists = s3.exists(path=TEST_TABLE_PATH_CSV)
    assert exists is True

    # Test.
    s3.download(
        from_path=TEST_TABLE_PATH_CSV,
        to_path="test.csv",
    )

    assert Path("test.csv").exists()

    # Cleanup.
    Path("test.csv").unlink()
    s3.rm(path=TEST_TABLE_PATH)


def test_if_exists(s3, TEST_CSV_FILE_PATH):
    """Test that `S3.exists()` works for files."""
    # Assumptions.
    exists = s3.exists(path=TEST_TABLE_PATH_CSV)
    assert exists is False

    # Test.
    s3.upload(
        from_path=TEST_CSV_FILE_PATH,
        to_path=TEST_TABLE_PATH_CSV,
    )
    exists = s3.exists(path=TEST_TABLE_PATH_CSV)
    assert exists is True

    # Cleanup.
    s3.rm(path=TEST_TABLE_PATH)


def test_if_exists_folder(s3, TEST_CSV_FILE_PATH):
    """Test that `S3.exists()` works for folders."""
    # Assumptions.
    exists = s3.exists(TEST_TABLE_PATH)
    assert exists is False

    # Test.
    s3.upload(
        from_path=TEST_CSV_FILE_PATH,
        to_path=TEST_TABLE_PATH_CSV,
    )
    exists = s3.exists(TEST_TABLE_PATH)
    assert exists is True

    # Cleanup.
    s3.rm(path=TEST_TABLE_PATH)
