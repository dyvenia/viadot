import json
import os
from datetime import timedelta

import pandas as pd
from prefect import Task
from prefect.utilities.tasks import defaults_from_attrs

from ..sources import AzureDataLake
from .azure_key_vault import ReadAzureKeyVaultSecret


class AzureDataLakeDownload(Task):
    """
    Task for downloading data from the Azure Data lakes (gen1 and gen2).

    Args:
        from_path (str, optional): The path from which to download the file(s). Defaults to None.
        to_path (str, optional): The destination path. Defaults to None.
        recursive (bool, optional): Set this to true if downloading entire directories.
        gen (int, optional): The generation of the Azure Data Lake. Defaults to 2.
        vault_name (str, optional): The name of the vault from which to fetch the secret. Defaults to None.
        max_retries (int, optional): [description]. Defaults to 3.
        retry_delay (timedelta, optional): [description]. Defaults to timedelta(seconds=10).
    """

    def __init__(
        self,
        from_path: str = None,
        to_path: str = None,
        recursive: bool = False,
        gen: int = 2,
        vault_name: str = None,
        max_retries: int = 3,
        retry_delay: timedelta = timedelta(seconds=10),
        *args,
        **kwargs,
    ):
        self.from_path = from_path
        self.to_path = to_path
        self.recursive = recursive
        self.gen = gen
        self.vault_name = vault_name

        super().__init__(
            name="adls_download",
            max_retries=max_retries,
            retry_delay=retry_delay,
            *args,
            **kwargs,
        )

    def __call__(self):
        """Download file(s) from the Azure Data Lake"""

    @defaults_from_attrs(
        "from_path",
        "to_path",
        "recursive",
        "gen",
        "vault_name",
        "max_retries",
        "retry_delay",
    )
    def run(
        self,
        from_path: str = None,
        to_path: str = None,
        recursive: bool = None,
        gen: int = None,
        sp_credentials_secret: str = None,
        vault_name: str = None,
        max_retries: int = None,
        retry_delay: timedelta = None,
    ):
        """Task run method.

        Args:
            from_path (str): The path from which to download the file(s).
            to_path (str): The destination path.
            recursive (bool): Set this to true if downloading entire directories.
            gen (int): The generation of the Azure Data Lake.
            sp_credentials_secret (str, optional): The name of the Azure Key Vault secret containing a dictionary with
            ACCOUNT_NAME and Service Principal credentials (TENANT_ID, CLIENT_ID, CLIENT_SECRET). Defaults to None.
            vault_name (str, optional): The name of the vault from which to obtain the secret. Defaults to None.
        """

        if sp_credentials_secret:
            azure_secret_task = ReadAzureKeyVaultSecret()
            credentials_str = azure_secret_task.run(
                secret=sp_credentials_secret, vault_name=vault_name
            )
            credentials = json.loads(credentials_str)
        else:
            credentials = {
                "ACCOUNT_NAME": os.environ["AZURE_ACCOUNT_NAME"],
                "AZURE_TENANT_ID": os.environ["AZURE_TENANT_ID"],
                "AZURE_CLIENT_ID": os.environ["AZURE_CLIENT_ID"],
                "AZURE_CLIENT_SECRET": os.environ["AZURE_CLIENT_SECRET"],
            }
        lake = AzureDataLake(gen=gen, credentials=credentials)

        full_dl_path = os.path.join(credentials["ACCOUNT_NAME"], from_path)
        self.logger.info(f"Downloading data from {full_dl_path} to {to_path}...")
        lake.download(from_path=from_path, to_path=to_path, recursive=recursive)
        self.logger.info(f"Successfully downloaded data to {to_path}.")


class AzureDataLakeUpload(Task):
    """Upload file(s) to Azure Data Lake.

    Args:
        from_path (str, optional): The local path from which to upload the file(s). Defaults to None.
        to_path (str, optional): The destination path. Defaults to None.
        recursive (bool, optional): Set this to true if uploading entire directories. Defaults to False.
        overwrite (bool, optional): Whether to overwrite files in the lake. Defaults to False.
        gen (int, optional): The generation of the Azure Data Lake. Defaults to 2.
        vault_name (str, optional): The name of the vault from which to obtain the secret. Defaults to None.
    """

    def __init__(
        self,
        from_path: str = None,
        to_path: str = None,
        recursive: bool = False,
        overwrite: bool = False,
        gen: int = 2,
        vault_name: str = None,
        max_retries: int = 3,
        retry_delay: timedelta = timedelta(seconds=10),
        *args,
        **kwargs,
    ):
        self.from_path = from_path
        self.to_path = to_path
        self.recursive = recursive
        self.overwrite = overwrite
        self.gen = gen
        self.vault_name = vault_name
        super().__init__(
            name="adls_upload",
            max_retries=max_retries,
            retry_delay=retry_delay,
            *args,
            **kwargs,
        )

    def __call__(self):
        """Upload file(s) to the Azure Data Lake"""

    @defaults_from_attrs(
        "from_path",
        "to_path",
        "recursive",
        "overwrite",
        "gen",
        "vault_name",
        "max_retries",
        "retry_delay",
    )
    def run(
        self,
        from_path: str = None,
        to_path: str = None,
        recursive: bool = None,
        overwrite: bool = None,
        gen: int = None,
        sp_credentials_secret: str = None,
        vault_name: str = None,
        max_retries: int = None,
        retry_delay: timedelta = None,
    ):
        """Task run method.

        Args:
            from_path (str): The path from which to upload the file(s).
            to_path (str): The destination path.
            recursive (bool): Set to true if uploading entire directories.
            overwrite (bool): Whether to overwrite the file(s) if they exist.
            gen (int): The generation of the Azure Data Lake.
            sp_credentials_secret (str, optional): The name of the Azure Key Vault secret containing a dictionary with
            ACCOUNT_NAME and Service Principal credentials (TENANT_ID, CLIENT_ID, CLIENT_SECRET). Defaults to None.
            vault_name (str, optional): The name of the vault from which to obtain the secret. Defaults to None.
        """

        if sp_credentials_secret:
            azure_secret_task = ReadAzureKeyVaultSecret()
            credentials_str = azure_secret_task.run(
                secret=sp_credentials_secret, vault_name=vault_name
            )
            credentials = json.loads(credentials_str)
        else:
            credentials = {
                "ACCOUNT_NAME": os.environ["AZURE_ACCOUNT_NAME"],
                "AZURE_TENANT_ID": os.environ["AZURE_TENANT_ID"],
                "AZURE_CLIENT_ID": os.environ["AZURE_CLIENT_ID"],
                "AZURE_CLIENT_SECRET": os.environ["AZURE_CLIENT_SECRET"],
            }
        lake = AzureDataLake(gen=gen, credentials=credentials)

        full_to_path = os.path.join(credentials["ACCOUNT_NAME"], to_path)
        self.logger.info(f"Uploading data from {from_path} to {full_to_path}...")
        lake.upload(
            from_path=from_path,
            to_path=to_path,
            recursive=recursive,
            overwrite=overwrite,
        )
        self.logger.info(f"Successfully uploaded data to {full_to_path}.")


class AzureDataLakeToDF(Task):
    def __init__(
        self,
        path: str = None,
        sep: str = "\t",
        gen: int = 2,
        vault_name: str = None,
        max_retries: int = 3,
        retry_delay: timedelta = timedelta(seconds=10),
        *args,
        **kwargs,
    ):
        """Load file(s) from the Azure Data Lake to a pandas DataFrame.

        Args:
            path (str, optional): The path from which to load the DataFrame. Defaults to None.
            sep (str, optional): The separator to use when reading the CSV file. Defaults to "\t".
            gen (int, optional): The generation of the Azure Data Lake. Defaults to 2.
            vault_name (str, optional): The name of the vault from which to obtain the secret. Defaults to None.
        """
        self.path = path
        self.sep = sep
        self.gen = gen
        self.vault_name = vault_name
        super().__init__(
            name="adls_to_df",
            max_retries=max_retries,
            retry_delay=retry_delay,
            *args,
            **kwargs,
        )

    def __call__(self):
        """Load file(s) from the Azure Data Lake to a pandas DataFrame."""

    @defaults_from_attrs(
        "path",
        "sep",
        "gen",
        "vault_name",
        "max_retries",
        "retry_delay",
    )
    def run(
        self,
        path: str = None,
        sep: str = None,
        gen: int = None,
        sp_credentials_secret: str = None,
        vault_name: str = None,
        max_retries: int = None,
        retry_delay: timedelta = None,
    ) -> pd.DataFrame:
        """Task run method.

        Args:
            path (str): The path to file(s) which should be loaded into a DataFrame.
            sep (str): The field separator to use when loading the file to the DataFrame.
            gen (int): The generation of the Azure Data Lake.
            sp_credentials_secret (str, optional): The name of the Azure Key Vault secret containing a dictionary with
            ACCOUNT_NAME and Service Principal credentials (TENANT_ID, CLIENT_ID, CLIENT_SECRET). Defaults to None.
            vault_name (str, optional): The name of the vault from which to obtain the secret. Defaults to None.
        """

        if sp_credentials_secret:
            azure_secret_task = ReadAzureKeyVaultSecret()
            credentials_str = azure_secret_task.run(
                secret=sp_credentials_secret, vault_name=vault_name
            )
            credentials = json.loads(credentials_str)
        else:
            credentials = {
                "ACCOUNT_NAME": os.environ["AZURE_ACCOUNT_NAME"],
                "AZURE_TENANT_ID": os.environ["AZURE_TENANT_ID"],
                "AZURE_CLIENT_ID": os.environ["AZURE_CLIENT_ID"],
                "AZURE_CLIENT_SECRET": os.environ["AZURE_CLIENT_SECRET"],
            }
        lake = AzureDataLake(gen=gen, credentials=credentials, path=path)

        full_dl_path = os.path.join(credentials["ACCOUNT_NAME"], path)
        self.logger.info(f"Downloading data from {full_dl_path} to a DataFrame...")
        df = lake.to_df(sep=sep)
        self.logger.info(f"Successfully loaded data.")
        return df
