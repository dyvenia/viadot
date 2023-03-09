import os

import pandas as pd
import pytest

from viadot.sources import S3

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

S3_BUCKET = os.environ.get("S3_BUCKET")
TEST_SCHEMA = "raw_test"
TEST_TABLE = "test"


@pytest.fixture(scope="session")
def s3(aws_config_key):
    s3 = S3(config_key=aws_config_key)

    yield s3


def test_from_df(s3):
    s3.from_df(df=TEST_DF, path=f"s3://{S3_BUCKET}/nesso/{TEST_SCHEMA}/{TEST_TABLE}")

    result = s3.exists(path=f"s3://{S3_BUCKET}/nesso/{TEST_SCHEMA}/{TEST_TABLE}")

    s3.rm(paths=[f"s3://{S3_BUCKET}/nesso/{TEST_SCHEMA}/{TEST_TABLE}"])

    assert result is True


def test_from_df_max_rows(s3):
    rows_per_file = 1
    size_df = len(TEST_DF)
    amount_of_expected_files = int(size_df / rows_per_file)

    s3.from_df(
        df=TEST_DF,
        path=f"s3://{S3_BUCKET}/nesso/{TEST_SCHEMA}/{TEST_TABLE}",
        max_rows_by_file=rows_per_file,
    )

    counter_files = 0

    for i in range(0, amount_of_expected_files, 1):
        if s3.exists(path=f"s3://{S3_BUCKET}/nesso/{TEST_SCHEMA}/{TEST_TABLE}_{i}"):
            counter_files = counter_files + 1
            s3.rm(paths=[f"s3://{S3_BUCKET}/nesso/{TEST_SCHEMA}/{TEST_TABLE}_{i}"])

    assert (counter_files == amount_of_expected_files) is True


def test_to_df(s3):
    s3.from_df(
        df=TEST_DF, path=f"s3://{S3_BUCKET}/nesso/{TEST_SCHEMA}/{TEST_TABLE}.parquet"
    )

    result = s3.to_df(
        paths=[f"s3://{S3_BUCKET}/nesso/{TEST_SCHEMA}/{TEST_TABLE}.parquet"]
    )

    assert len(result) == len(TEST_DF)

    # Cleanup.
    s3.rm(paths=[f"s3://{S3_BUCKET}/nesso/{TEST_SCHEMA}/{TEST_TABLE}.parquet"])


def test_to_df_chunk_size(s3):
    s3.from_df(
        df=TEST_DF, path=f"s3://{S3_BUCKET}/nesso/{TEST_SCHEMA}/{TEST_TABLE}.parquet"
    )

    dfs = s3.to_df(
        paths=[f"s3://{S3_BUCKET}/nesso/{TEST_SCHEMA}/{TEST_TABLE}.parquet"],
        chunk_size=1,
    )

    result = pd.concat(dfs, ignore_index=True)
    assert len(result) == len(TEST_DF)

    # Cleanup.
    s3.rm(paths=[f"s3://{S3_BUCKET}/nesso/{TEST_SCHEMA}/{TEST_TABLE}.parquet"])


def test_upload(s3):
    TEST_DF.to_csv("test.csv")

    s3.upload(
        from_path="test.csv",
        to_path=f"s3://{S3_BUCKET}/nesso/{TEST_SCHEMA}/{TEST_TABLE}.csv",
    )

    result = s3.exists(path=f"s3://{S3_BUCKET}/nesso/{TEST_SCHEMA}/{TEST_TABLE}.csv")

    os.remove("test.csv")

    assert result is True


def test_download(s3):
    s3.download(
        to_path="test.csv",
        from_path=f"s3://{S3_BUCKET}/nesso/{TEST_SCHEMA}/{TEST_TABLE}.csv",
    )

    result = os.path.exists("test.csv")

    s3.rm(paths=[f"s3://{S3_BUCKET}/nesso/{TEST_SCHEMA}/{TEST_TABLE}.csv"])

    os.remove("test.csv")

    assert result is True
