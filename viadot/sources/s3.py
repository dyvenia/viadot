from typing import Iterable, List, Literal, Union

import awswrangler as wr
import boto3
import pandas as pd
import s3fs
from pydantic import BaseModel

from viadot.config import get_source_credentials
from viadot.sources.base import Source


class S3Credentials(BaseModel):
    profile_name: str  # The name of the IAM profile to use.
    region_name: str  # The name of the AWS region.
    aws_access_key_id: str
    aws_secret_access_key: str


class S3(Source):
    """
    A class for pulling data from and uploading to the Amazon S3.

    Args:
        credentials (S3Credentials, optional): Amazon S3 credentials.
            Defaults to None.
        config_key (str, optional): The key in the viadot config holding relevant
            credentials. Defaults to None.
    """

    def __init__(
        self,
        credentials: S3Credentials = None,
        config_key: str = None,
        *args,
        **kwargs,
    ):
        credentials = credentials or get_source_credentials(config_key) or {}

        super().__init__(*args, credentials=credentials, **kwargs)

        if not self.credentials:
            self.logger.debug(
                "Credentials not specified. Falling back to `boto3` default credentials."
            )

        self.fs = s3fs.S3FileSystem(
            profile=self.credentials.get("profile_name"),
            region_name=self.credentials.get("region_name"),
            key=self.credentials.get("aws_access_key_id"),
            secret=self.credentials.get("aws_secret_access_key"),
        )

        self._session = None

    @property
    def session(self) -> boto3.session.Session:
        """A singleton-like property for initiating a session to the AWS."""
        if not self._session:
            self._session = boto3.session.Session(
                region_name=self.credentials.get("region_name"),
                profile_name=self.credentials.get("profile_name"),
                aws_access_key_id=self.credentials.get("aws_access_key_id"),
                aws_secret_access_key=self.credentials.get("aws_secret_access_key"),
            )
        return self._session

    def ls(self, path: str, suffix: str = None) -> List[str]:
        """
        Returns a list of objects in a provided path.

        Args:
            path (str): Path to a folder.
            suffix (Union[str, List[str], None]) - Suffix or List of suffixes for
                filtering Amazon S3 keys. Defaults to None.
        """

        return wr.s3.list_objects(boto3_session=self.session, path=path, suffix=suffix)

    def exists(self, path: str) -> bool:
        """
        Check if an object exists in the Amazon S3.

        Args:
            path (str): The path to an object to check.

        Returns:
            bool: Whether the object exists.
        """
        return wr.s3.does_object_exist(boto3_session=self.session, path=path)

    def cp(self, from_path: str, to_path: str, recursive: bool = False) -> None:
        """
        Copies the contents of `from_path` to `to_path`.

        Args:
            from_path (str, optional): The path (S3 URL) of the source directory.
            to_path (str, optional): The path (S3 URL) of the target directory.
            recursive (bool, optional): Set this to true if working with directories.
                Defaults to False.

        Example:
            Copy files within two S3 locations:

            ```python
            from viadot.sources.s3 import S3

            s3 = S3()
            s3.cp(
                from_path="s3://bucket-name/folder_a/",
                to_path="s3://bucket-name/folder_b/",
                recursive=True
            )
        """
        self.fs.copy(path1=from_path, path2=to_path, recursive=recursive)

    def rm(self, paths: list[str]) -> None:
        """
        Delete files under `paths`.

        Args:
            paths (list[str]): Paths to files or folders to be removed. If the path
                is a directory, it will be removed recursively.
        ```python
        from viadot.sources import S3

        s3 = S3()
        s3.rm(paths=["file1.parquet"])
        ```
        """

        wr.s3.delete_objects(boto3_session=self.session, path=paths)

    def from_df(
        self,
        df: pd.DataFrame,
        path: str,
        extension: Literal[".csv", ".parquet"] = ".parquet",
        **kwargs,
    ) -> None:
        """
        Upload a pandas `DataFrame` into Amazon S3 as a CSV or Parquet file.
        For full list of available parameters please refer to the official documentation:
        https://aws-sdk-pandas.readthedocs.io/en/3.0.0/stubs/awswrangler.s3.to_parquet.html
        https://aws-sdk-pandas.readthedocs.io/en/3.0.0/stubs/awswrangler.s3.to_csv.html

        Args:
            df (pd.DataFrame): The pandas DataFrame to upload.
            path (str): The destination path.
            extension (Literal[".csv", ".parquet"]): The file extension. Either ".csv"
                or ".parquet". Defaults to ".parquet".
        """

        if extension == ".parquet":
            wr.s3.to_parquet(
                boto3_session=self.session,
                df=df,
                path=path,
                **kwargs,
            )
        elif extension == ".csv":
            wr.s3.to_csv(
                boto3_session=self.session,
                df=df,
                path=path,
                **kwargs,
            )
        else:
            raise ValueError("Only parquet and CSV formats are supported.")

    def to_df(
        self,
        paths: list[str],
        chunk_size: int = None,
        **kwargs,
    ) -> Union[pd.DataFrame, Iterable[pd.DataFrame]]:
        """
        Reads a CSV or Parquet file into a pandas `DataFrame`.

        Args:
            paths (list[str]): A list of paths to Amazon S3 files. All files under the path
                must be of the same type.
            chunk_size (int, optional): Number of rows to include in each chunk.
                Defaults to None, ie. return all data as a single `DataFrame`.

        Returns:
            Union[pd.DataFrame, Iterable[pd.DataFrame]]: A pandas DataFrame
                or an iterable of pandas DataFrames.

        Example 1:
        ```python
        from viadot.sources import S3

        s3 = S3()
        paths = ["s3://{bucket}/file1.parquet", "s3://{bucket}/file2.parquet"]
        s3.to_df(paths=paths)
        ```

        Example 2:
        ```python
        from viadot.sources import S3

        s3 = S3()
        paths = ["s3://{bucket}/file1.parquet", "s3://{bucket}/file2.parquet"]
        dfs = s3.to_df(paths=paths, chunk_size=2)

        for df in dfs:
            print(df)
        ```
        """

        if chunk_size is None:
            # `chunked` expects either an integer or a boolean.
            chunk_size = False

        if paths[0].endswith(".csv"):
            df = wr.s3.read_csv(
                boto3_session=self.session, path=paths, chunked=chunk_size, **kwargs
            )
        elif paths[0].endswith(".parquet"):
            df = wr.s3.read_parquet(
                boto3_session=self.session, path=paths, chunked=chunk_size, **kwargs
            )
        else:
            raise ValueError("Only CSV and parquet formats are supported.")
        return df

    def upload(self, from_path: str, to_path: str) -> None:
        """
        Upload file(s) to S3.

        Args:
            from_path (str): Path to local file(s) to be uploaded.
            to_path (str): Path to the destination file/folder.
        """

        wr.s3.upload(boto3_session=self.session, local_file=from_path, path=to_path)

    def download(self, from_path: str, to_path: str) -> None:
        """
        Download file(s) from Amazon S3.

        Args:
            from_path (str): Path to file in Amazon S3.
            to_path (str): Path to local file(s) to be stored.
        """

        wr.s3.download(boto3_session=self.session, path=from_path, local_file=to_path)
