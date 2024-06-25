"""Task for uploading pandas DataFrame to MinIO."""

import pandas as pd
from typing import Any, Literal
import contextlib


from prefect import task
from prefect.logging import get_run_logger

with contextlib.suppress(ImportError):
    from viadot.sources import MinIO

from viadot.orchestration.prefect.utils import get_credentials
from viadot.config import get_source_credentials
from viadot.orchestration.prefect.exceptions import MissingSourceCredentialsError


@task(retries=3, retry_delay_seconds=10, timeout_seconds=60 * 60)
def df_to_minio(
    df: pd.DataFrame,
    path: str,
    credentials: dict[str, Any] | None = None,
    credentials_secret: str | None = None,
    config_key: str | None = None,
    basename_template: str | None = None,
    if_exists: Literal["error", "delete_matching", "overwrite_or_ignore"] = "error",
) -> None:
    """Task to upload a file to MinIO.

    Args:
        df (pd.DataFrame): Pandas dataframe to be uploaded.
        path (str): Path to the MinIO file/folder.
        credentials (dict[str, Any], optional): Credentials to the MinIO.
            Defaults to None.
        credentials_secret (str, optional): The name of the secret storing
            the credentials. Defaults to None.
            More info on: https://docs.prefect.io/concepts/blocks/
        config_key (str, optional): The key in the viadot config holding relevant
            credentials. Defaults to None.
        basename_template (str, optional): A template string used to generate
                basenames of written data files. The token ‘{i}’ will be replaced with
                an automatically incremented integer. Defaults to None.
        if_exists (Literal["error", "delete_matching", "overwrite_or_ignore"],
            optional). What to do if the dataset already exists. Defaults to "error".


    """
    if not (credentials_secret or credentials or config_key):
        raise MissingSourceCredentialsError

    logger = get_run_logger()

    credentials = (
        credentials
        or get_source_credentials(config_key)
        or get_credentials(credentials_secret)
    )
    minio = MinIO(
        credentials=credentials,
    )

    minio.from_df(
        df=df, path=path, if_exists=if_exists, basename_template=basename_template
    )

    logger.info("Data has been uploaded successfully.")