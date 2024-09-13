"""Tasks for interacting with Azure Data Lake (gen2)."""

import contextlib
import os
from pathlib import Path

import numpy as np
import pandas as pd
from prefect import get_run_logger, task

from viadot.orchestration.prefect.exceptions import MissingSourceCredentialsError
from viadot.orchestration.prefect.utils import get_credentials

with contextlib.suppress(ImportError):
    from viadot.sources import AzureDataLake


@task(retries=3, retry_delay_seconds=10, timeout_seconds=60 * 60)
def adls_upload(
    to_path: str,
    from_path: str | None = None,
    recursive: bool = False,
    overwrite: bool = False,
    credentials_secret: str | None = None,
    config_key: str | None = None,
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
    """
    if not (credentials_secret or config_key):
        raise MissingSourceCredentialsError

    credentials = get_credentials(credentials_secret)
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
    sep: str = "\t",
    credentials_secret: str | None = None,
    config_key: str | None = None,
    overwrite: bool = False,
) -> None:
    r"""Upload a pandas `DataFrame` to a file on Azure Data Lake.

    Args:
        df (pd.DataFrame): The pandas DataFrame to upload.
        path (str): The destination path. Defaults to None.
        sep (str, optional): The separator to use in the `to_csv` function. Defaults to
            "\t".
        overwrite (bool, optional): Whether to overwrite files in the lake. Defaults
            to False.
        credentials_secret (str, optional): The name of the Azure Key Vault secret
            storing the credentials.
        config_key (str, optional): The key in the viadot config holding relevant
            credentials. Defaults to None.
    """
    if not (credentials_secret or config_key):
        raise MissingSourceCredentialsError

    credentials = get_credentials(credentials_secret)
    lake = AzureDataLake(credentials=credentials, config_key=config_key)

    lake.from_df(
        df=df,
        path=path,
        sep=sep,
        overwrite=overwrite,
    )


@task(retries=3, retry_delay_seconds=10)
def adls_to_df(
    path: str,
    sep: str = "\t",
    credentials_secret: str | None = None,
    config_key: str | None = None,
    **kwargs,
) -> pd.DataFrame:
    r"""Load file(s) from the Azure Data Lake to a pandas DataFrame.

    Note: Currently supports CSV and parquet files.

    Args:
        path (str): The path from which to load the DataFrame.
        sep (str, optional): The separator to use when reading a CSV file.
            Defaults to "\t".
        credentials_secret (str, optional): The name of the Azure Key Vault secret
            storing the credentials.
        config_key (str, optional): The key in the viadot config holding relevant
            credentials.

    Raises:
        MissingSourceCredentialsError: If credentials were not provided.

    Returns:
        pd.DataFrame: The HTTP response object.
    """
    logger = get_run_logger()

    if not (credentials_secret or config_key):
        raise MissingSourceCredentialsError

    credentials = get_credentials(credentials_secret)
    lake = AzureDataLake(credentials=credentials, config_key=config_key)

    full_dl_path = str(Path(credentials["ACCOUNT_NAME"], path))
    logger.info(f"Downloading data from {full_dl_path} to a DataFrame...")

    name = Path(path).stem
    suffixes = "".join(Path(path).suffixes)
    file_name = f"{name}{suffixes}"

    lake.download(to_path=file_name, from_path=path, recursive=False)

    if ".csv" in suffixes:
        df = pd.read_csv(file_name, sep=sep)
    elif ".parquet" in suffixes:
        df = pd.read_parquet(file_name)

    os.remove(file_name)

    logger.info("Successfully loaded data.")

    return df


@task(retries=3, retry_delay_seconds=10)
def adls_list(
    path: str,
    recursive: bool = False,
    file_to_match: str | None = None,
    credentials_secret: str | None = None,
    config_key: str | None = None,
) -> list[str]:
    """Consult list of files in Azure Data Lake Storage.

    Args:
        path (str): The path to the directory which contents you want to list.
        recursive (bool, optional): If True, recursively list all subdirectories
            and files. Defaults to False.
        file_to_match (str, optional): If exist it only returns files with that name.
            Defaults to None.
        credentials_secret (str, optional): The name of the Azure Key Vault secret
            storing the credentials.
        config_key (str, optional): The key in the viadot config holding relevant
            credentials.

    Raises:
        MissingSourceCredentialsError: If credentials were not provided.

    Returns:
        list[str]: The list of paths to the contents of `path`. These paths
        do not include the container, eg. the path to the file located at
        "https://my_storage_acc.blob.core.windows.net/raw/supermetrics/test_file.txt"
        will be shown as "raw/supermetrics/test_file.txt".
    """
    logger = get_run_logger()

    if not (credentials_secret or config_key):
        raise MissingSourceCredentialsError

    credentials = get_credentials(credentials_secret)
    lake = AzureDataLake(credentials=credentials, config_key=config_key)

    full_dl_path = str(Path(credentials["ACCOUNT_NAME"], path))

    logger.info(f"Listing files in {full_dl_path}.")
    if recursive:
        logger.info("Loading ADLS directories recursively.")
        files = lake.ls(path)
        if file_to_match:
            conditions = [file_to_match in item for item in files]
            valid_files = np.array([])
            if any(conditions):
                index = np.where(conditions)[0]
                files = list(np.append(valid_files, [files[i] for i in index]))
            else:
                message = f"There are not any available file named {file_to_match}."
                raise FileExistsError(message)
    else:
        files = lake.ls(path)

    logger.info(f"Successfully listed files in {full_dl_path}.")

    return files
