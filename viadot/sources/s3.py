from typing import Any, Dict, List, Union, Literal

import awswrangler as wr
import boto3
import pandas as pd
import s3fs

from viadot.config import get_source_credentials
from viadot.sources.base import Source


class S3(Source):
    """
    A class for pulling data from and uploading to the S3.

    Args:
        credentials (Dict[str, Any], optional): Credentials to the AWS S3.
            Defaults to None.
        config_key (str, optional): The key in the viadot config holding relevant
            credentials. Defaults to None.
    """

    def __init__(
        self,
        credentials: Dict[str, Any] = None,
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
            region_name=self.credentials.get("region_name"),
            profile=self.credentials.get("profile_name"),
            key=self.credentials.get("aws_access_key_id"),
            secret=self.credentials.get("aws_secret_access_key"),
        )

        self._session = None

    @property
    def session(self):
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
        Returns a list of files in a S3.

        Args:
            path (str): Path to a folder.
            suffix (Union[str, List[str], None]) - Suffix or List of suffixes for
                filtering S3 keys. Defaults to None.
        """

        return wr.s3.list_objects(boto3_session=self.session, path=path, suffix=suffix)

    def exists(self, path: str) -> bool:
        """
        Check if a location exists in S3.

        Args:
            path (str): The path to check. Can be a file or a directory.
        Returns:
            bool: Whether the paths exists.
        """
        return wr.s3.does_object_exist(boto3_session=self.session, path=path)

    def cp(self, from_path: str, to_path: str, recursive: bool = False):
        """
        Copies the contents of `from_path` to `to_path`.

        Args:
            from_path (str, optional): S3 Path for the source directory.
            to_path (str, optional): S3 Path for the target directory.
            recursive (bool, optional): Set this to true if working with directories.
                Defaults to False.

        Example:
            Copy files within two S3 locations:

            ```python

            from viadot.sources.s3 import S3

            s3 = S3()
            s3.cp(
                from_path='s3://bucket-name/folder_a/',
                to_path='s3://bucket-name/folder_b/',
                recursive=True
            )
        """
        self.fs.copy(path1=from_path, path2=to_path, recursive=recursive)

    def rm(self, paths: list[str]):
        """
        Deletes files in a path.

        Args:
            paths (list[str]): Path to a file or folder to be removed. If the path refers to
                a folder, it will be removed recursively. Also the possibilty to delete multiple files at once by
                passing the individual paths as a list of strings.
        ```python
        from viadot.sources import S3
        s3 = S3()
        s3.rm(
            paths=['path_first_file', 'path_second_file']
        )
        ```
        """

        wr.s3.delete_objects(boto3_session=self.session, path=paths)

    def from_df(
        self,
        df: pd.DataFrame,
        path: str,
        extension: Literal[".csv", ".parquet"] = ".parquet",
        max_rows_by_file: int = None,
        **kwargs,
    ):
        """
        Upload a pandas `DataFrame` to a csv or parquet file. You can choose different
            file backends, and have the option of compression.

        Args:
            df (pd.DataFrame): Pandas DataFrame.
            path (str): Path to a S3 folder.
            extension (Literal[".csv", ".parquet"]): Required file type. Accepted file formats are 'csv'
                and 'parquet'. Defaults to '.parquet'.
            max_rows_by_file (int): Max number of rows in each file (only works for read_parquet). Default to None.
        """

        if extension == ".csv":
            wr.s3.to_csv(
                boto3_session=self.session,
                df=df,
                path=path,
                **kwargs,
            )
        else:
            wr.s3.to_parquet(
                boto3_session=self.session,
                df=df,
                path=path,
                max_rows_by_file=max_rows_by_file,
                **kwargs,
            )

    def to_df(
        self,
        paths: list[str],
        chunked: Union[int, bool] = False,
        **kwargs,
    ):
        """
        Reads a csv or parquet file to a pd.DataFrame. Possibility of reading in several files in one or multiple pd.DataFrames.

        Args:
            paths (list[str]): A list of paths to S3 files. List must contain a uniform file format.
            chunked (Union[int, bool], optional): If True data will be split in a Iterable of DataFrames (Memory friendly).
                If Integer data will be intereated by number of rows equal to the received Integer.

        Example 1:
        ```python
        from viadot.sources import S3
        s3 = S3()
        s3.to_df(paths=['s3://{bucket}/pathfirstfile.parquet', 's3://{bucket}/pathsecondfile.parquet'])
        ```

        Example 2:
        ```python
        from viadot.sources import S3
        s3 = S3()
        dfs = s3.to_df(paths=['s3://{bucket}/pathfirstfile.parquet', 's3://{bucket}/pathsecondfile.parquet'], chunked=True)
        for df in dfs:
            print(df)
        ```
        """

        if paths[0].endswith(".csv"):
            df = wr.s3.read_csv(
                boto3_session=self.session, path=paths, chunked=chunked, **kwargs
            )
        elif paths[0].endswith(".parquet"):
            df = wr.s3.read_parquet(
                boto3_session=self.session, path=paths, chunked=chunked, **kwargs
            )
        else:
            raise ValueError("Only CSV and parquet formats are supported.")
        return df

    def upload(self, from_path: str, to_path: str):
        """
        Upload file(s) to S3.

        Args:
            from_path (str): Path to local file(s) to be uploaded.
            to_path (str): Path to the destination file/folder.
        """

        wr.s3.upload(boto3_session=self.session, local_file=from_path, path=to_path)

    def download(self, from_path: str, to_path: str):
        """
        Download file(s) from S3.

        Args:
            from_path (str): Path to file in S3.
            to_path (str): Path to local file(s) to be stored.
        """

        wr.s3.download(boto3_session=self.session, path=from_path, local_file=to_path)
