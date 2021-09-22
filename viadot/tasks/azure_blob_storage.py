from prefect import Task

from ..sources import AzureBlobStorage

# from ..tasks import AzureKeyVaultSecret


class BlobFromCSV(Task):
    def __init__(self, *args, **kwargs):
        super().__init__(name="csv_to_blob_storage", *args, **kwargs)

    def __call__(self):
        """Generate a blob from a local CSV file"""

    def run(
        self,
        from_path: str,
        to_path: str,
        overwrite: bool = False,
        sp_credentials_secret: str = None,
        vault_name: str = None,
    ):
        """[summary]

        Args:
            from_path (str): The path from which to download the file(s). Defaults to None.
            to_path (str): The destination path. Defaults to None.
            overwrite (bool): Indicator what to do if file already exists. Defaults to False.
            sp_credentials_secret (str, optional): The name of the Azure KeyVault secret containing a dictionary with
            Service Principal credentials (TENANT_ID, CLIENT_ID, CLIENT_SECRET). Defaults to None.
            vault_name (str, optional): The name of the vault from which to obtain the secret. Defaults to None.
        """

        # if sp_credentials_secret:
        #     azure_secret_task = AzureKeyVaultSecret()
        #     credentials = azure_secret_task.run(
        #         secret=sp_credentials_secret, vault_name=vault_name
        #     )
        # else:
        #     credentials = None
        # blob_storage = AzureBlobStorage(credentials=credentials)

        blob_storage = AzureBlobStorage()

        self.logger.info(f"Copying from {from_path} to {to_path}...")

        result = blob_storage.to_storage(
            from_path=from_path, to_path=to_path, overwrite=overwrite
        )
        self.logger.info(f"Successfully uploaded data to {to_path}.")

        return result
