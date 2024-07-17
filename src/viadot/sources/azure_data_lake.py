import os
from typing import Any, Dict, List

import pandas as pd

try:
    from adlfs import AzureBlobFileSystem, AzureDatalakeFileSystem
except ModuleNotFoundError:
    raise ImportError("Missing required modules to use AzureDataLake source.")

from viadot.config import get_source_credentials
from viadot.exceptions import CredentialError
from viadot.sources.base import Source
from viadot.utils import add_viadot_metadata_columns


class AzureDataLake(Source):
    """
    A class for pulling data from the Azure Data Lakes (gen1 and gen2).

    You can either connect to the lake in general
    (`lake = AzureDataLake(); lake.exists("a/b/c.csv")`),
    or to a particular path (`lake = AzureDataLake(path="a/b/c.csv"); lake.exists()`)

    Args:
        credentials (Dict[str, Any], optional): A dictionary containing
            the following credentials: `account_name`, `tenant_id`,
            `client_id`, and `client_secret`.
        config_key (str, optional): The key in the viadot config holding
            relevant credentials.
    """

    def __init__(
        self,
        path: str = None,
        gen: int = 2,
        credentials: Dict[str, Any] = None,
        config_key: str = None,
        *args,
        **kwargs,
    ):
        credentials = credentials or get_source_credentials(config_key)
        # pass to lower letters
        credentials = {key.lower(): value for key, value in credentials.items()}
        
        required_credentials = (
            "account_name",
            "azure_tenant_id",
            "azure_client_id",
            "azure_client_secret",
        )
        required_credentials_are_provided = all(
            [rc in credentials for rc in required_credentials]
        )

        if credentials is None:
            raise CredentialError("Please provide the credentials.")
        elif not required_credentials_are_provided:
            raise CredentialError("Please provide all required credentials.")

        super().__init__(*args, credentials=credentials, **kwargs)

        storage_account_name = self.credentials["account_name"]
        tenant_id = self.credentials["azure_tenant_id"]
        client_id = self.credentials["azure_client_id"]
        client_secret = self.credentials["azure_client_secret"]
 
        self.path = path
        self.gen = gen
        self.storage_options = {
            "azure_tenant_id": tenant_id,
            "azure_client_id": client_id,
            "azure_client_secret": client_secret,
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
            self.base_url = "az://"

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
            raise NotImplementedError(
                "Azure Data Lake Gen1 does not support simple file upload."
            )

        to_path = to_path or self.path

        self.logger.info(f"Uploading file(s) from '{from_path}' to '{to_path}'...")

        try:
            self.fs.upload(
                lpath=from_path,
                rpath=to_path,
                recursive=recursive,
                overwrite=overwrite,
            )
        except FileExistsError:
            # Show a useful error message.
            if recursive:
                msg = f"At least one file in '{to_path}' already exists. Specify `overwrite=True` to overwrite."  # noqa
            else:
                msg = f"The file '{to_path}' already exists. Specify `overwrite=True` to overwrite."
            raise FileExistsError(msg)

        self.logger.info(
            f"Successfully uploaded file(s) from '{from_path}' to '{to_path}'."
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
            raise NotImplementedError(
                "Currently, only the default behavior (overwrite) is available."
            )

        from_path = from_path or self.path
        self.fs.download(rpath=from_path, lpath=to_path, recursive=recursive)

    @add_viadot_metadata_columns
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
        """
        Returns a list of files in a path.

        Args:
            path (str, optional): Path to a folder. Defaults to None.
        """
        path = path or self.path
        return self.fs.ls(path)

    def rm(self, path: str = None, recursive: bool = False):
        """
        Deletes files in a path.

        Args:
            path (str, optional): Path to a folder. Defaults to None.
            recursive (bool, optional): Whether to delete files recursively.
                Defaults to False.
        """
        path = path or self.path
        self.fs.rm(path, recursive=recursive)

    def cp(self, from_path: str = None, to_path: str = None, recursive: bool = False):
        """
        Copies the contents of `from_path` to `to_path`.

        Args:
            from_path (str, optional): Path from which to copy file(s).
                Defauls to None.
            to_path (str, optional): Path where to copy file(s). Defaults
                to None.
            recursive (bool, optional): Whether to copy file(s) recursively.
                Defaults to False.
        """
        from_path = from_path or self.path
        to_path = to_path
        self.fs.cp(from_path, to_path, recursive=recursive)

    def from_df(
        self, df: pd.DataFrame, path: str = None, overwrite: bool = False
    ) -> None:
        """
        Upload a pandas `DataFrame` to a file on Azure Data Lake.

        Args:
            df (pd.DataFrame): The pandas `DataFrame` to upload.
            path (str, optional): The destination path. Defaults to None.
            overwrite (bool): Whether to overwrite the file if it exist.
        """

        path = path or self.path

        extension = path.split(".")[-1]
        if extension not in ("csv", "parquet"):
            if "." not in path:
                msg = "Please provide the full path to the file."
            else:
                msg = "Accepted file formats are 'csv' and 'parquet'."
            raise ValueError(msg)

        file_name = path.split("/")[-1]
        if extension == "csv":
            # Can do it simply like this if ADLS accesses are set up correctly
            # url = os.path.join(self.base_url, path)
            # df.to_csv(url, storage_options=self.storage_options)
            df.to_csv(file_name, index=False)
        else:
            df.to_parquet(file_name, index=False)

        self.upload(from_path=file_name, to_path=path, overwrite=overwrite)

        os.remove(file_name)
