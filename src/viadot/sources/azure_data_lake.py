"""A module for working with Azure Data Lake (gen1 and gen2)."""

from pathlib import Path
from typing import Any

import pandas as pd


try:
    from adlfs import AzureBlobFileSystem, AzureDatalakeFileSystem
except ModuleNotFoundError as e:
    msg = "Missing required modules to use AzureDataLake source."
    raise ImportError(msg) from e

from viadot.config import get_source_credentials
from viadot.exceptions import CredentialError
from viadot.sources.base import Source
from viadot.utils import add_viadot_metadata_columns


class AzureDataLake(Source):
    def __init__(
        self,
        path: str | None = None,
        gen: int = 2,
        credentials: dict[str, Any] | None = None,
        config_key: str | None = None,
        *args,
        **kwargs,
    ):
        """A class for pulling data from the Azure Data Lakes (gen1 and gen2).

        You can either connect to the lake in general:
        lake = AzureDataLake(); lake.exists("a/b/c.csv")

        or to a particular path:
        lake = AzureDataLake(path="a/b/c.csv"); lake.exists()`

        Args:
        credentials (Dict[str, Any], optional): A dictionary containing
            the following credentials: `account_name`, `tenant_id`,
            `client_id`, and `client_secret`.
        config_key (str, optional): The key in the viadot config holding
            relevant credentials.
        """
        credentials = credentials or get_source_credentials(config_key)
        credentials = {key.lower(): value for key, value in credentials.items()}

        required_credentials = (
            "account_name",
            "azure_tenant_id",
            "azure_client_id",
            "azure_client_secret",
        )
        required_credentials_are_provided = all(
            rc in credentials for rc in required_credentials
        )

        if credentials is None:
            msg = "Please provide the credentials."
            raise CredentialError(msg)

        if not required_credentials_are_provided:
            msg = "Please provide all required credentials."
            raise CredentialError(msg)

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
        elif gen == 2:  # noqa: PLR2004
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
        to_path: str | None = None,
        recursive: bool = False,
        overwrite: bool = False,
    ) -> None:
        """Upload file(s) to the lake.

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
            msg = "Azure Data Lake Gen1 does not support simple file upload."
            raise NotImplementedError(msg)

        to_path = to_path or self.path

        self.logger.info(f"Uploading file(s) from '{from_path}' to '{to_path}'...")

        try:
            self.fs.upload(
                lpath=from_path,
                rpath=to_path,
                recursive=recursive,
                overwrite=overwrite,
            )
        except FileExistsError as e:
            if recursive:
                msg = f"At least one file in '{to_path}' already exists. Specify `overwrite=True` to overwrite."
            else:
                msg = f"The file '{to_path}' already exists. Specify `overwrite=True` to overwrite."
            raise FileExistsError(msg) from e

        self.logger.info(
            f"Successfully uploaded file(s) from '{from_path}' to '{to_path}'."
        )

    def exists(self, path: str | None = None) -> bool:
        """Check if a location exists in Azure Data Lake.

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
        from_path: str | None = None,
        recursive: bool = False,
        overwrite: bool = True,
    ) -> None:
        """Download file(s) from the lake.

        Args:
            to_path (str): _description_
            from_path (str | None, optional): _description_. Defaults to None.
            recursive (bool, optional): _description_. Defaults to False.
            overwrite (bool, optional): _description_. Defaults to True.

        Raises:
            NotImplementedError: _description_
        """
        if overwrite is False:
            msg = "Currently, only the default behavior (overwrite) is available."
            raise NotImplementedError(msg)

        from_path = from_path or self.path
        self.fs.download(rpath=from_path, lpath=to_path, recursive=recursive)

    @add_viadot_metadata_columns
    def to_df(
        self,
        path: str | None = None,
        sep: str = "\t",
        quoting: int = 0,
        lineterminator: str | None = None,
        error_bad_lines: bool | None = None,
    ) -> pd.DataFrame:
        r"""Download a file from the lake and return it as a pandas DataFrame.

        Args:
            path (str, optional): _description_. Defaults to None.
            sep (str, optional): _description_. Defaults to "\t".
            quoting (int, optional): _description_. Defaults to 0.
            lineterminator (str, optional): _description_. Defaults to None.
            error_bad_lines (bool, optional): _description_. Defaults to None.

        Raises:
            ValueError: _description_

        Returns:
            _type_: _description_
        """
        if quoting is None:
            quoting = 0

        path = path or self.path
        url = self.base_url + "/" + path.strip("/")

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
            msg = "Only CSV and parquet formats are supported."
            raise ValueError(msg)

        return df

    def ls(self, path: str | None = None) -> list[str]:
        """Returns a list of files in a path.

        Args:
            path (str, optional): Path to a folder. Defaults to None.
        """
        path = path or self.path
        return self.fs.ls(path)

    def rm(self, path: str | None = None, recursive: bool = False) -> None:
        """Delete files in a path.

        Args:
            path (str, optional): Path to a folder. Defaults to None.
            recursive (bool, optional): Whether to delete files recursively.
                Defaults to False.
        """
        path = path or self.path
        self.fs.rm(path, recursive=recursive)

    def cp(
        self,
        from_path: str | None = None,
        to_path: str | None = None,
        recursive: bool = False,
    ) -> None:
        """Copies the contents of `from_path` to `to_path`.

        Args:
            from_path (str, optional): Path from which to copy file(s). Defaults to
                None.
            to_path (str, optional): Path where to copy file(s). Defaults to None.
            recursive (bool, optional): Whether to copy file(s) recursively. Defaults to
                False.
        """
        from_path = from_path or self.path
        self.fs.cp(from_path, to_path, recursive=recursive)

    def from_df(
        self,
        df: pd.DataFrame,
        path: str | None = None,
        sep: str = "\t",
        overwrite: bool = False,
    ) -> None:
        r"""Upload a pandas `DataFrame` to a file on Azure Data Lake.

        Args:
            df (pd.DataFrame): The pandas `DataFrame` to upload.
            path (str, optional): The destination path. Defaults to None.
            sep (str, optional): The separator to use in the `to_csv()` function.
                Defaults to "\t".
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
            df.to_csv(file_name, index=False, sep=sep)
        else:
            df.to_parquet(file_name, index=False)

        self.upload(from_path=file_name, to_path=path, overwrite=overwrite)

        Path(file_name).unlink()
