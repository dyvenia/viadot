import os
from typing import Any, Dict, List

import pandas as pd
from adlfs import AzureBlobFileSystem, AzureDatalakeFileSystem

from ..config import local_config
from .base import Source


class AzureDataLake(Source):
    """
    A class for pulling data from the Azure Data Lakes (gen1 and gen2).
    You can either connect to the lake in general or to a particular path,
    eg.
    lake = AzureDataLake(); lake.exists("a/b/c.csv")
    vs
    lake = AzureDataLake(path="a/b/c.csv"); lake.exists()

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
        self,
        path: str = None,
        gen: int = 2,
        credentials: Dict[str, Any] = None,
        *args,
        **kwargs,
    ):

        credentials = credentials or local_config.get("AZURE_ADLS")

        super().__init__(*args, credentials=credentials, **kwargs)

        storage_account_name = self.credentials["ACCOUNT_NAME"]
        tenant_id = self.credentials["AZURE_TENANT_ID"]
        client_id = self.credentials["AZURE_CLIENT_ID"]
        client_secret = self.credentials["AZURE_CLIENT_SECRET"]

        self.path = path
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
            self.base_url = f"adl://{storage_account_name}"
        elif gen == 2:
            self.storage_options["account_name"] = storage_account_name
            self.fs = AzureBlobFileSystem(
                account_name=storage_account_name,
                tenant_id=tenant_id,
                client_id=client_id,
                client_secret=client_secret,
            )
            self.base_url = f"az://"

    def upload(
        self,
        from_path: str,
        to_path: str = None,
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

        to_path = to_path or self.path
        self.fs.upload(
            lpath=from_path,
            rpath=to_path,
            recursive=recursive,
            overwrite=overwrite,
        )

    def exists(self, path: str = None) -> bool:
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
        path = path or self.path
        return self.fs.exists(path)

    def download(
        self,
        to_path: str,
        from_path: str = None,
        recursive: bool = False,
        overwrite: bool = True,
    ) -> None:
        if overwrite is False:
            raise NotImplemented(
                "Currently, only the default behavior (overwrite) is available."
            )

        from_path = from_path or self.path
        self.fs.download(rpath=from_path, lpath=to_path, recursive=recursive)

    def to_df(
        self,
        path: str = None,
        sep: str = "\t",
        quoting: int = 0,
        lineterminator: str = None,
        error_bad_lines: bool = None,
    ):
        if quoting is None:
            quoting = 0

        path = path or self.path
        url = os.path.join(self.base_url, path)

        if url.endswith(".csv"):
            df = pd.read_csv(
                url,
                storage_options=self.storage_options,
                sep=sep,
                quoting=quoting,
                lineterminator=lineterminator,
                error_bad_lines=error_bad_lines,
            )
        elif url.endswith(".parquet"):
            df = pd.read_parquet(url, storage_options=self.storage_options)
        else:
            raise ValueError("Only CSV and parquet formats are supported.")

        return df

    def ls(self, path: str = None) -> List[str]:
        path = path or self.path
        return self.fs.ls(path)

    def rm(self, path: str = None, recursive: bool = False):
        path = path or self.path
        self.fs.rm(path, recursive=recursive)

    def cp(self, from_path: str = None, to_path: str = None, recursive: bool = False):
        from_path = from_path or self.path
        to_path = to_path
        self.fs.cp(from_path, to_path, recursive=recursive)
