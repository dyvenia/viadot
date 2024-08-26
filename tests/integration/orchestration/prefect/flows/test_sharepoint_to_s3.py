import os
from pathlib import Path

from viadot.orchestration.prefect.flows import sharepoint_to_s3
from viadot.sources import S3


S3_BUCKET = os.environ.get("S3_BUCKET")
TEST_SCHEMA = "raw_test"
TEST_TABLE = "test_sharepoint_to_s3"


def test_sharepoint_to_s3(sharepoint_url, sharepoint_config_key):
    file_extension = sharepoint_url.split(".")[-1]
    local_path = "sharepoint_test" + file_extension
    s3_path = f"s3://{S3_BUCKET}/nesso/{TEST_SCHEMA}/{TEST_TABLE}." + file_extension

    sharepoint_to_s3(
        url=sharepoint_url,
        local_path=local_path,
        to_path=s3_path,
        sharepoint_config_key=sharepoint_config_key,
    )

    Path(local_path).unlink()

    s3 = S3()
    file_exists = s3.exists(path=s3_path)

    assert file_exists is True

    s3.rm(paths=[s3_path])
