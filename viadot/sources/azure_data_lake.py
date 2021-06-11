from adlfs import AzureDatalakeFileSystem, AzureBlobFileSystem

from typing import Any, Dict, List

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

        credentials = credentials or {}

        super().__init__(*args, credentials=credentials, **kwargs)

        storage_account_name = self.credentials["ACCOUNT_NAME"]
        tenant_id = self.credentials["AZURE_TENANT_ID"]
        client_id = self.credentials["AZURE_CLIENT_ID"]
        client_secret = self.credentials["AZURE_CLIENT_SECRET"]

        self.gen = gen
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

    def upload(
        self,
        from_path: str,
        to_path: str,
        recursive: bool = False,
        overwrite: bool = False,
    ) -> None:
        """
        Upload file(s) to the lake.

        Args:
            from_path (str): Path to the local file(s) to be uploaded.
            to_path (str): Path to the destination file/folder
            recursive (bool): Set this to true if working with directories.
            overwrite (bool): Whether to overwrite the file(s) if they exist.

        Example:
        ```python
        from viadot.sources import AzureDataLake
        lake = AzureDataLake()
        lake.upload(from_path='tests/test.csv', to_path="sandbox/test.csv")
        ```
        """

        if self.gen == 1:
            raise NotImplemented(
                "Azure Data Lake Gen1 does not support simple file upload."
            )

        self.fs.upload(
            lpath=from_path, rpath=to_path, recursive=recursive, overwrite=overwrite
        )

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

    def download(
        self,
        from_path: str,
        to_path: str,
        recursive: bool = False,
        overwrite: bool = True,
    ) -> None:
        if overwrite is False:
            raise NotImplemented(
                "Currently, only the default behavior (overwrite) is available."
            )

        self.fs.download(rpath=from_path, lpath=to_path, recursive=recursive)

    def ls(self, path: str) -> List[str]:
        return self.fs.ls(path)
