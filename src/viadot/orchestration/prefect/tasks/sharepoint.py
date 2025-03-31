"""Tasks for interacting with Microsoft Sharepoint."""

from typing import Any

import pandas as pd
from prefect import get_run_logger, task

from viadot.orchestration.prefect.exceptions import MissingSourceCredentialsError
from viadot.orchestration.prefect.utils import get_credentials
from viadot.sources import Sharepoint, SharepointList


@task(retries=3, retry_delay_seconds=10, timeout_seconds=60 * 60)
def sharepoint_to_df(
    url: str,
    sheet_name: str | list[str | int] | int | None = None,
    columns: str | list[str] | list[int] | None = None,
    tests: dict[str, Any] | None = None,
    file_sheet_mapping: dict | None = None,
    na_values: list[str] | None = None,
    credentials_secret: str | None = None,
    config_key: str | None = None,
) -> pd.DataFrame:
    """Load an Excel file stored on Microsoft Sharepoint into a pandas `DataFrame`.

    Modes:
    If the `URL` ends with the file (e.g ../file.xlsx) it downloads only the file and
    creates a DataFrame from it.
    If the `URL` ends with the folder (e.g ../folder_name/): it downloads multiple files
    and creates a DataFrame from them:
        - If `file_sheet_mapping` is provided, it downloads and processes only
            the specified files and sheets.
        - If `file_sheet_mapping` is NOT provided, it downloads and processes all of
            the files from the chosen folder.

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
        file_sheet_mapping (dict): A dictionary where keys are filenames and values are
            the sheet names to be loaded from each file. If provided, only these files
            and sheets will be downloaded. Defaults to None.
        na_values (list[str] | None): Additional strings to recognize as NA/NaN.
            If list passed, the specific NA values for each column will be recognized.
            Defaults to None.
        tests (dict[str], optional): A dictionary with optional list of tests
                to verify the output dataframe. If defined, triggers the `validate`
                function from viadot.utils. Defaults to None.
        config_key (str, optional): The key in the viadot config holding relevant
            credentials. Defaults to None.

    Returns:
        pd.Dataframe: The pandas `DataFrame` containing data from the file.
    """
    if not (credentials_secret or config_key):
        raise MissingSourceCredentialsError

    logger = get_run_logger()

    credentials = get_credentials(secret_name=credentials_secret)
    s = Sharepoint(credentials=credentials, config_key=config_key)

    logger.info(f"Downloading data from {url}...")
    df = s.to_df(
        url,
        sheet_name=sheet_name,
        tests=tests,
        usecols=columns,
        na_values=na_values,
        file_sheet_mapping=file_sheet_mapping,
    )
    logger.info(f"Successfully downloaded data from {url}.")

    return df


@task(retries=3, retry_delay_seconds=10, timeout_seconds=60 * 60)
def sharepoint_download_file(
    url: str,
    to_path: str,
    credentials_secret: str | None = None,
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
    if not (credentials_secret or config_key):
        raise MissingSourceCredentialsError

    logger = get_run_logger()

    credentials = get_credentials(secret_name=credentials_secret)
    s = Sharepoint(credentials=credentials, config_key=config_key)

    logger.info(f"Downloading data from {url}...")
    s.download_file(url=url, to_path=to_path)
    logger.info(f"Successfully downloaded data from {url}.")


@task(retries=3, retry_delay_seconds=10, timeout_seconds=60 * 60)
def sharepoint_list_to_df(
    list_name: str,
    list_site: str,
    default_protocol: str | None = "https://",
    query: str | None = None,
    select: list[str] | None = None,
    credentials_secret: str | None = None,
    config_key: str | None = None,
    tests: dict[str, Any] | None = None,
) -> pd.DataFrame:
    """Retrieve data from a SharePoint list into a pandas DataFrame.

    Args:
        list_name (str): The name of the SharePoint list.
        list_site (str): The Sharepoint site on which the list is stored.
        default_protocol (str, optional): The default protocol to use for
                SharePoint URLs.
                Defaults to "https://".
        query (str, optional): A query to filter items. Defaults to None.
        select (list[str], optional): Fields to include in the response.
            Defaults to None.
        credentials_secret (str, optional): The name of the secret storing the
        credentials.Defaults to None.
        config_key (str, optional): The key in the viadot config holding relevant
            credentials. Defaults to None.
        tests (dict[str], optional): A dictionary with optional list of tests
                to verify the output dataframe. If defined, triggers the `validate`
                function from viadot.utils. Defaults to None.

    Returns:
        pd.DataFrame: The DataFrame containing data from the SharePoint list.

    Raises:
        MissingSourceCredentialsError: If neither credentials_secret nor
            config_key is provided.
    """
    if not (credentials_secret or config_key):
        raise MissingSourceCredentialsError

    logger = get_run_logger()

    credentials = get_credentials(secret_name=credentials_secret)
    sp = SharepointList(
        credentials=credentials,
        config_key=config_key,
        default_protocol=default_protocol,
    )

    logger.info(f"Retrieving data from SharePoint list {list_name}...")
    df = sp.to_df(
        list_name=list_name,
        query=query,
        select=select,
        list_site=list_site,
        tests=tests,
    )
    logger.info(f"Successfully retrieved data from SharePoint list {list_name}.")

    return df
