import os
from pathlib import Path

import pandas as pd
import pytest
from orchestration.prefect_viadot.tasks import s3_upload_file
from prefect import flow

try:
    from viadot.sources import S3

    _s3_installed = True
except ImportError:
    _s3_installed = False

if not _s3_installed:
    pytest.skip("S3 source not installed", allow_module_level=True)

S3_BUCKET = os.environ.get("S3_BUCKET")
TEST_SCHEMA = "raw_test"
TEST_TABLE = "test_s3_upload_file"


@pytest.fixture(scope="session")
def s3(aws_config_key):
    return S3(config_key=aws_config_key)


@pytest.fixture()
def TEST_FILE_PATH():
    path = "test.csv"
    df = pd.DataFrame(
        [
            [0, "A"],
            [1, "B"],
            [2, "C"],
        ],
        columns=["col1", "col2"],
    )
    df.to_csv(path)

    yield path

    Path(path).unlink()


def test_s3_upload_file(TEST_FILE_PATH):
    @flow
    def test_flow():
        file_path = f"s3://{S3_BUCKET}/nesso/{TEST_SCHEMA}/{TEST_TABLE}.csv"
        s3_upload_file(
            from_path=TEST_FILE_PATH,
            to_path=file_path,
        )

        file_exists = s3.exists(path=file_path)

        s3.rm(paths=[file_path])

        return file_exists

    assert test_flow() is True
