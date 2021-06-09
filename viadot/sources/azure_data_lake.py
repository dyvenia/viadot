from adlfs import AzureDatalakeFileSystem, AzureBlobFileSystem

from typing import Any, Dict

from ..config import local_config
from .base import Source


class AzureDataLake(Source):
    """
    A class for pulling data from the Azure Data Lakes (gen1 and gen2).

    Parameters
    ----------
    credentials : Dict[str, Any], optional
        A dictionary containing ACCOUNT_NAME and the following
        Service Principal credentials:
            - AZURE_TENANT_ID
            - AZURE_CLIENT_ID
            - AZURE_CLIENT_SECRET
    """

    def __init__(
        self, credentials: Dict[str, Any] = None, gen: int = 2, *args, **kwargs
    ):
        DEFAULT_CREDENTIALS = local_config.get("AZURE_BLOB_STORAGE")
        credentials = credentials or DEFAULT_CREDENTIALS
        super().__init__(*args, credentials=credentials, **kwargs)
        storage_account_name = self.credentials["ACCOUNT_NAME"]
        tenant_id = self.credentials["AZURE_TENANT_ID"]
        client_id = self.credentials["AZURE_CLIENT_ID"]
        client_secret = self.credentials["AZURE_CLIENT_SECRET"]

        self.storage_options = {
            "tenant_id": tenant_id,
            "client_id": client_id,
            "client_secret": client_secret,
        }
        if gen == 1:
            self.fs = AzureDatalakeFileSystem(
                store_name=storage_account_name,
                tenant_id=tenant_id,
                client_id=client_id,
                client_secret=client_secret,
            )
        elif gen == 2:
            self.fs = AzureBlobFileSystem(
                account_name=storage_account_name,
                tenant_id=tenant_id,
                client_id=client_id,
                client_secret=client_secret,
            )

    def upload(self, from_path: str, to_path: str, overwrite: bool = False):
        raise NotImplemented
        # """Upload a file to the lake.

        # Args:
        #     from_path (str): Path to the local file to be uploaded.
        #     to_path (str): The destination path in the format 'filesystem/path/a.csv'
        #     overwrite (bool, optional): Whether to overwrite the file if it exists.
        #     Defaults to False.

        # Example:
        # ```python
        # from viadot.sources import AzureDataLake
        # lake = AzureDataLake()
        # lake.upload('tests/test.csv', to_path="tests/test.csv")
        # ```

        # Returns:
        #     bool: Whether the operation was successful.
        # """

        # file_system_name = to_path.split("/")[0]
        # file_system_client = self.service_client.get_file_system_client(
        #     file_system_name
        # )

        # file_path = "/".join(to_path.split("/")[1:])
        # file_client = file_system_client.get_file_client(file_path)

        # file_client = file_client.create_file()
        # with open(from_path, "rb") as f:
        #     file_content = f.read()

        # file_client.upload_data(data=file_content, overwrite=overwrite)

        # return True

    def exists(self, path: str) -> bool:
        """
        Check if a location exists in Azure Data Lake.

        Args:
            path (str): The path to check. Can be a file or a directory.

        Example:
        ```python
        from viadot.sources import AzureDataLake

        lake = AzureDataLake(gen=1)
        lake.exists("tests/test.csv")
        ```

        Returns:
            bool: Whether the paths exists.
        """
        return self.fs.exists(path)

    def download(self, from_path, to_path: str, recursive: bool = False):
        self.fs.download(rpath=from_path, lpath=to_path, recursive=recursive)

    def ls(self, path: str):
        return self.fs.ls(path)
