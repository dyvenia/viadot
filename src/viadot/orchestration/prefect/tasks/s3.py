"""Task for uploading pandas DataFrame to Amazon S3."""

import contextlib
from typing import Any

from prefect import task
from prefect.logging import get_run_logger


with contextlib.suppress(ImportError):
    from viadot.sources import S3


@task(retries=3, retry_delay_seconds=10, timeout_seconds=60 * 60)
def s3_upload_file(
    from_path: str,
    to_path: str,
    credentials: dict[str, Any] | None = None,
    config_key: str | None = None,
) -> None:
    """Task to upload a file to Amazon S3.

    Args:
        from_path (str): Path to local file(s) to be uploaded.
        to_path (str): Path to the Amazon S3 file/folder.
        credentials (dict[str, Any], optional): Credentials to the Amazon S3.
            Defaults to None.
        config_key (str, optional): The key in the viadot config holding relevant
            credentials. Defaults to None.

    Example:
        ```python
        from prefect_viadot.tasks import s3_upload_file
        from prefect import flow

        @flow
        def test_flow():
            s3_upload_file(
                from_path='test.parquet',
                to_path="s3://bucket_name/test.parquet",
                credentials= {
                    'profile_name': 'your_profile'
                    'region_name': 'your_region'
                    'aws_access_key_id': 'your_access_key_id'
                    'aws_secret_access_key': 'your_secret_access_key'
                }
            )

        test_flow()
        ```
    """
    s3 = S3(credentials=credentials, config_key=config_key)

    s3.upload(from_path=from_path, to_path=to_path)

    logger = get_run_logger()
    logger.info("Data has been uploaded successfully.")
