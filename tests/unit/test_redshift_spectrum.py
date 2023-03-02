import os

import pandas as pd
import pytest

from viadot.sources import S3, RedshiftSpectrum

TEST_DF = pd.DataFrame(
    [
        [0, "A"],
        [1, "B"],
        [2, "C"],
    ],
    columns=["col1", "col2"],
)

S3_BUCKET = os.environ.get("S3_BUCKET")
TEST_SCHEMA = "raw_test"
TEST_TABLE = "test_redshift_spectrum"


@pytest.fixture(scope="session")
def redshift(aws_config_key):
    redshift = RedshiftSpectrum(config_key=aws_config_key)

    yield redshift


@pytest.fixture(scope="session")
def s3(aws_config_key):
    s3 = S3(config_key=aws_config_key)

    yield s3


def test_from_df(redshift):
    redshift.from_df(
        df=TEST_DF,
        to_path=f"s3://datawerfen-159170848751291/nesso/{TEST_SCHEMA}/{TEST_TABLE}",
        database=TEST_SCHEMA,
        table=TEST_TABLE,
    )

    result = redshift.exists(
        database=TEST_SCHEMA,
        table=TEST_TABLE,
    )
    redshift.drop_table(database=TEST_SCHEMA, table=TEST_TABLE, remove_files=True)

    assert result is True


def test_to_df(redshift):
    redshift.from_df(
        df=TEST_DF,
        to_path=f"s3://datawerfen-159170848751291/nesso/{TEST_SCHEMA}/{TEST_TABLE}",
        database=TEST_SCHEMA,
        table=TEST_TABLE,
    )

    result = redshift.to_df(
        database=TEST_SCHEMA,
        table=TEST_TABLE,
    )

    assert len(result) == len(TEST_DF)

    redshift.drop_table(database=TEST_SCHEMA, table=TEST_TABLE, remove_files=True)
