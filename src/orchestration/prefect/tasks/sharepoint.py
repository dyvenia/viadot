"""Tasks for interacting with Microsoft Sharepoint."""

from typing import Any

import pandas as pd
from prefect import get_run_logger, task
from viadot.sources import Sharepoint
from viadot.sources.sharepoint import SharepointCredentials

from prefect_viadot.exceptions import MissingSourceCredentialsError
from prefect_viadot.utils import get_credentials


@task(retries=3, retry_delay_seconds=10, timeout_seconds=60 * 60)
def sharepoint_to_df(
    url: str,
    sheet_name: str | list[str | int] | int | None = None,
    columns: str | list[str] | list[int] | None = None,
    tests: dict[str, Any] | None = None,
    credentials_secret: str | None = None,
    config_key: str | None = None,
    credentials: dict[str, Any] | None = None,
) -> pd.DataFrame:
    """Load an Excel file stored on Microsoft Sharepoint into a pandas `DataFrame`.

    Args:
        url (str): The URL to the file.
        sheet_name (str | list | int, optional): Strings are used
            for sheet names. Integers are used in zero-indexed sheet positions
            (chart sheets do not count as a sheet position). Lists of strings/integers
            are used to request multiple sheets. Specify None to get all worksheets.
            Defaults to None.
        columns (str | list[str] | list[int], optional): Which columns to ingest.
            Defaults to None.
        credentials_secret (str, optional): The name of the secret storing
            the credentials. Defaults to None.
            More info on: https://docs.prefect.io/concepts/blocks/
        tests (dict[str], optional): A dictionary with optional list of tests
                to verify the output dataframe. If defined, triggers the `validate`
                function from viadot.utils. Defaults to None.
        config_key (str, optional): The key in the viadot config holding relevant
            credentials. Defaults to None.
        credentials (dict, optional): The credentials as a dictionary. Defaults
            to None.

    Returns:
        pd.Dataframe: The pandas `DataFrame` containing data from the file.
    """
    if not (credentials_secret or config_key or credentials):
        raise MissingSourceCredentialsError

    logger = get_run_logger()

    credentials = credentials or get_credentials(credentials_secret)
    s = Sharepoint(credentials=credentials, config_key=config_key)

    logger.info(f"Downloading data from {url}...")
    df = s.to_df(url, sheet_name=sheet_name, tests=tests, usecols=columns)
    logger.info(f"Successfully downloaded data from {url}.")

    return df


@task
def sharepoint_download_file(
    url: str,
    to_path: str,
    credentials_secret: str | None = None,
    credentials: SharepointCredentials | None = None,
    config_key: str | None = None,
) -> None:
    """Download a file from Sharepoint.

    Args:
        url (str): The URL of the file to be downloaded.
        to_path (str): Where to download the file.
        credentials_secret (str, optional): The name of the secret that stores
            Sharepoint credentials. Defaults to None.
        credentials (SharepointCredentials, optional): Sharepoint credentials.
        config_key (str, optional): The key in the viadot config holding relevant
            credentials.
    """
    if not (credentials_secret or config_key or credentials):
        raise MissingSourceCredentialsError

    logger = get_run_logger()

    credentials = credentials or get_credentials(credentials_secret)
    s = Sharepoint(credentials=credentials, config_key=config_key)

    logger.info(f"Downloading data from {url}...")
    s.download_file(url=url, to_path=to_path)
    logger.info(f"Successfully downloaded data from {url}.")
