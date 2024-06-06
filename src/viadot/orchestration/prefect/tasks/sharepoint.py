"""Tasks for interacting with Microsoft Sharepoint."""

from typing import Any
from urllib.parse import urlparse

import pandas as pd
from viadot.orchestration.prefect.exceptions import MissingSourceCredentialsError
from viadot.orchestration.prefect.utils import get_credentials
from viadot.sources import Sharepoint
from viadot.sources.sharepoint import SharepointCredentials, get_last_segment_from_url

from prefect import get_run_logger, task


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

    credentials = credentials or get_credentials(secret_name=credentials_secret)
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

    credentials = credentials or get_credentials(secret_name=credentials_secret)
    s = Sharepoint(credentials=credentials, config_key=config_key)

    logger.info(f"Downloading data from {url}...")
    s.download_file(url=url, to_path=to_path)
    logger.info(f"Successfully downloaded data from {url}.")


@task
def scan_sharepoint_folder(
    url: str,
    credentials_secret: str | None = None,
    credentials: SharepointCredentials | None = None,
    config_key: str | None = None,
) -> list[str]:
    """Scan Sharepoint folder to get all file URLs.

    Args:
        url (str): The URL of the folder to scan.
        credentials_secret (str, optional): The name of the secret that stores
            Sharepoint credentials. Defaults to None.
        credentials (SharepointCredentials, optional): Sharepoint credentials.
        config_key (str, optional): The key in the viadot config holding relevant
            credentials.

    Raises:
        MissingSourceCredentialsError: Raise when no source credentials were provided.
        ValueError: Raises when URL have the wrong structure - without 'sites' segment.

    Returns:
        list[str]: List of URLS.
    """
    if not (credentials_secret or config_key or credentials):
        raise MissingSourceCredentialsError

    credentials = credentials or get_credentials(secret_name=credentials_secret)
    s = Sharepoint(credentials=credentials, config_key=config_key)
    conn = s.get_connection()
    parsed_url = urlparse(url)
    path_parts = parsed_url.path.split("/")
    if "sites" in path_parts:
        site_index = (
            path_parts.index("sites") + 2
        )  # +2 to include 'sites' and the next segment
        site_url = f"{parsed_url.scheme}://{parsed_url.netloc}{'/'.join(path_parts[:site_index])}"
        library = "/".join(path_parts[site_index:])
    else:
        message = "URL does not contain '/sites/' segment."
        raise ValueError(message)

    # -> site_url = company.sharepoint.com/sites/site_name/
    # -> library = /shared_documents/folder/sub_folder/final_folder
    endpoint = f"{site_url}/_api/web/GetFolderByServerRelativeUrl('{library}')/Files"
    response = conn.get(endpoint)
    files = response.json().get("d", {}).get("results", [])

    return [f'{site_url}/{library}{file["Name"]}' for file in files]


@task
def validate_and_reorder_dfs_columns(dataframes_list: list[pd.DataFrame]) -> None:
    """Validate if dataframes from the list have the same column structure.

    Reorder columns to match the first DataFrame if necessary.

    Args:
        dataframes_list (list[pd.DataFrame]): List containing DataFrames.

    Raises:
        IndexError: If the list of DataFrames is empty.
        ValueError: If DataFrames have different column structures.
    """
    logger = get_run_logger()

    if not dataframes_list:
        message = "The list of dataframes is empty."
        raise IndexError(message)

    first_df_columns = dataframes_list[0].columns

    # Check that all DataFrames have the same columns
    for i, df in enumerate(dataframes_list):
        if set(df.columns) != set(first_df_columns):
            logger.info(
                f"""Columns from first dataframe: {first_df_columns}\n
                        Columns from DataFrame at index {i}: {df.columns} """
            )
            message = f"""DataFrame at index {i} does not have the same structure as
            the first DataFrame."""
            raise ValueError(message)
        if not df.columns.equals(first_df_columns):
            logger.info(
                f"Reordering columns for DataFrame at index {i} to match the first DataFrame."
            )
            dataframes_list[i] = df.loc[:, first_df_columns]

    logger.info("DataFrames have been validated and columns reordered where necessary.")
    return dataframes_list


@task
def get_endpoint_type_from_url(url: str) -> str:
    """Get the type of the last segment of the URL.

    This function uses `get_last_segment_from_url` to parse the provided URL and
    determine whether the last segment represents a file or a directory.

    Args:
        url (str): The URL to a SharePoint file or directory.

    Returns:
        str: A string indicating the type of the last segment, either 'file' or
        'directory'.
    """
    _, endpoint_type = get_last_segment_from_url(url)
    return endpoint_type
