"""Tasks for interacting with Azure Data Lake (gen2)."""

from typing import Any

import pandas as pd
from prefect import task
from viadot.sources import AzureDataLake

from prefect_viadot.exceptions import MissingSourceCredentialsError
from prefect_viadot.utils import get_credentials


@task(retries=3, retry_delay_seconds=10, timeout_seconds=60 * 60)
def adls_upload(
    to_path: str,
    from_path: str | None = None,
    recursive: bool = False,
    overwrite: bool = False,
    credentials_secret: str | None = None,
    config_key: str | None = None,
    credentials: dict[str, Any] | None = None,
) -> None:
    """Upload file(s) to Azure Data Lake.

    Credentials can be specified as either a key inside viadot config file,
    or the name of the Prefect `AzureKeyVaultSecretReference` block document
    storing the reference to an Azure Key Vault secret.

    Args:
        to_path (str, optional): The destination path.
        recursive (bool, optional): Set this to true if uploading entire directories.
            Defaults to False.
        from_path (str, optional): The local path from which to upload the file(s).
            Defaults to None.
        overwrite (bool, optional): Whether to overwrite files in the lake. Defaults
            to False.
        credentials_secret (str, optional): The name of the Azure Key Vault secret
            storing the credentials.
        config_key (str, optional): The key in the viadot config holding relevant
            credentials.
        credentials (dict, optional): The credentials as a dictionary.
    """
    if not (credentials_secret or config_key or credentials):
        raise MissingSourceCredentialsError

    credentials = credentials or get_credentials(credentials_secret)
    lake = AzureDataLake(credentials=credentials, config_key=config_key)

    lake.upload(
        from_path=from_path,
        to_path=to_path,
        recursive=recursive,
        overwrite=overwrite,
    )


@task(retries=3, retry_delay_seconds=10)
def df_to_adls(
    df: pd.DataFrame,
    path: str,
    credentials_secret: str | None = None,
    config_key: str | None = None,
    credentials: dict[str, Any] | None = None,
    overwrite: bool = False,
) -> None:
    """Upload a pandas `DataFrame` to a file on Azure Data Lake.

    Args:
        df (pd.DataFrame): The pandas DataFrame to upload.
        path (str): The destination path. Defaults to None.
        overwrite (bool, optional): Whether to overwrite files in the lake. Defaults
            to False.
        credentials_secret (str, optional): The name of the Azure Key Vault secret
            storing the credentials.
        config_key (str, optional): The key in the viadot config holding relevant
            credentials. Defaults to None.
        credentials (dict, optional): The credentials as a dictionary.
    """
    if not (credentials_secret or config_key or credentials):
        raise MissingSourceCredentialsError

    credentials = credentials or get_credentials(credentials_secret)
    lake = AzureDataLake(credentials=credentials, config_key=config_key)

    lake.from_df(
        df=df,
        path=path,
        overwrite=overwrite,
    )
