import json
import os

from prefect import Task
from prefect.utilities.tasks import defaults_from_attrs

from ..sources import AzureDataLake
from .azure_key_vault import ReadAzureKeyVaultSecret


class AzureDataLakeDownload(Task):
    def __init__(
        self,
        from_path: str = None,
        to_path: str = None,
        recursive: bool = False,
        gen: int = 2,
        vault_name: str = None,
        *args,
        **kwargs,
    ):
        self.from_path = from_path
        self.to_path = to_path
        self.recursive = recursive
        self.gen = gen
        self.vault_name = vault_name
        super().__init__(name="adls_download", *args, **kwargs)

    def __call__(self):
        """Download file(s) from the Azure Data Lake"""

    @defaults_from_attrs("from_path", "to_path", "recursive", "gen", "vault_name")
    def run(
        self,
        from_path: str = None,
        to_path: str = None,
        recursive: bool = None,
        gen: int = None,
        sp_credentials_secret: str = None,
        vault_name: str = None,
    ):
        """Download file(s) from Azure Data Lake.

        Args:
            from_path (str): The path from which to download the file(s).
            to_path (str): The destination path.
            recursive (bool): Set to true if downloading entire directories.
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

        full_dl_path = credentials["ACCOUNT_NAME"] + from_path
        self.logger.info(f"Downloading data from {full_dl_path} to {to_path}...")
        lake.download(from_path=from_path, to_path=to_path, recursive=recursive)
        self.logger.info(f"Successfully downloaded data to {to_path}.")
