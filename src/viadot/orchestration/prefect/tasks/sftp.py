"""Tasks from SFTP API."""

import pandas as pd
from prefect import task

from viadot.orchestration.prefect.exceptions import MissingSourceCredentialsError
from viadot.orchestration.prefect.utils import get_credentials
from viadot.sources import Sftp


@task(retries=3, log_prints=True, retry_delay_seconds=10, timeout_seconds=60 * 60)
def sftp_to_df(
    config_key: str | None = None,
    azure_key_vault_secret: str | None = None,
    file_name: str | None = None,
    sep: str = "\t",
    columns: list[str] | None = None,
) -> pd.DataFrame:
    r"""Querying SFTP server and saving data as the data frame.

    Args:
        config_key (str, optional): The key in the viadot config holding relevant
            credentials. Defaults to None.
        azure_key_vault_secret (str, optional): The name of the Azure Key Vault secret
            where credentials are stored. Defaults to None.
        file_name (str, optional): Path to the file in SFTP server. Defaults to None.
        sep (str, optional): The separator to use to read the CSV file.
            Defaults to "\t".
        columns (List[str], optional): Columns to read from the file. Defaults to None.

    Returns:
        pd.DataFrame: The response data as a pandas DataFrame.
    """
    if not (azure_key_vault_secret or config_key):
        raise MissingSourceCredentialsError

    if not config_key:
        credentials = get_credentials(azure_key_vault_secret)

    sftp = Sftp(
        credentials=credentials,
        config_key=config_key,
    )
    sftp.get_connection()

    return sftp.to_df(file_name=file_name, sep=sep, columns=columns)


@task(retries=3, log_prints=True, retry_delay_seconds=10, timeout_seconds=60 * 60)
def sftp_list(
    config_key: str | None = None,
    azure_key_vault_secret: str | None = None,
    path: str | None = None,
    recursive: bool = False,
    matching_path: str | None = None,
) -> list[str]:
    """Listing files in the SFTP server.

    Args:
        config_key (str, optional): The key in the viadot config holding relevant
            credentials. Defaults to None.
        azure_key_vault_secret (str, optional): The name of the Azure Key Vault secret
            where credentials are stored. Defaults to None.
        path (str, optional): Destination path from where to get the structure.
            Defaults to None.
        recursive (bool, optional): Get the structure in deeper folders.
            Defaults to False.
        matching_path (str, optional): Filtering folders to return by a regex pattern.
            Defaults to None.

    Returns:
        files_list (list[str]): List of files in the SFTP server.
    """
    if not (azure_key_vault_secret or config_key):
        raise MissingSourceCredentialsError

    if not config_key:
        credentials = get_credentials(azure_key_vault_secret)

    sftp = Sftp(
        credentials=credentials,
        config_key=config_key,
    )
    sftp.get_connection()

    return sftp.get_files_list(
        path=path, recursive=recursive, matching_path=matching_path
    )
