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

    def rm(self, path: Union[List[str], str]):
        """
        Deletes files in a path.

        Args:
            path (Union[List[str], str]): Path to a file or folder to be removed. If the path refers to
                a folder, it will be removed recursively. Also the possibilty to delete multiple files at once by
                passing the individual paths as string within a list.
        ```python
        from viadot.sources.s3 import S3
        s3 = S3()
        s3.rm(
            path = ['path_first_file', 'path_second_file']
        )
        ```
        """

        wr.s3.delete_objects(boto3_session=self.session, path=path)

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
                ax_rows_by_file=max_rows_by_file,
                **kwargs,
            )

    def to_df(
        self,
        path: Union[List[str], str],
        chunked: Union[int, bool] = False,
        **kwargs,
    ):
        """
        Reads a csv or parquet file to a pd.DataFrame.

        Args:
            path (Union[List[str], str]): Individual or list of paths to S3 files.
            chunked (Union[int, bool], optional): If True data will be split in a Iterable of DataFrames (Memory friendly).
                                                  If an INTEGER is passed awswrangler will iterate on the data by number of rows equal to the received INTEGER.
        Example:
         ```python
        from viadot.sources.s3 import S3
        s3 = S3()
        # for chunked = False
        s3.to_df(path='s3://{bucket}/path.parquet')
        s3.to_df(path=['s3://{bucket}/pathfirstfile.parquet', 's3://{bucket}/pathsecondfile.parquet'])
        # for chunked = True/Integer
        dfs = s3.to_df(path=['s3://{bucket}/pathfirstfile.parquet', 's3://{bucket}/pathsecondfile.parquet'], chunked = True/Integer)
        for df in dfs:
            print(df)
        ```

        """
        path_first_entry = path

        if type(path_first_entry) is list:
            path_first_entry = path_first_entry[0]

        if path_first_entry.endswith(".csv"):
            df = wr.s3.read_csv(
                boto3_session=self.session, path=path, chunked=chunked, **kwargs
            )
        elif path_first_entry.endswith(".parquet"):
            df = wr.s3.read_parquet(
                boto3_session=self.session, path=path, chunked=chunked, **kwargs
            )
        else:
            raise ValueError("Only CSV and parquet formats are supported.")
        return df
