from typing import List

import awswrangler as wr
import boto3
import pandas as pd
import s3fs

from viadot.sources.base import Source


class S3(Source):
    """
    A class for pulling data from and uploading to S3.

    Args:
        profile_name (str, optional): The name of the AWS profile. Defaults to None.
        aws_access_key_id (str, optional): AWS access key id. Defaults to None.
        aws_secret_access_key (str, optional): AWS secret access key. Defaults to None.
    """

    def __init__(
        self,
        profile_name: str = None,
        aws_access_key_id: str = None,
        aws_secret_access_key: str = None,
    ):
        self.fs = s3fs.S3FileSystem(
            profile=profile_name,
            key=aws_access_key_id,
            secret=aws_secret_access_key,
        )

        self.profile_name = profile_name
        self.aws_access_key_id = aws_access_key_id
        self.aws_secret_access_key = aws_secret_access_key

        self._session = None

    @property
    def session(self):
        """A singleton-like property for initiating a session to the AWS."""
        if not self._session:
            self._session = boto3.session.Session(
                profile_name=self.profile_name,
                aws_access_key_id=self.aws_access_key_id,
                aws_secret_access_key=self.aws_secret_access_key,
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

            s3_session = S3()
            s3_session.cp(
                from_path='s3://bucket-name/folder_a/',
                to_path='s3://bucket-name/folder_b/',
                recursive=True
            )
        """
        self.fs.copy(path1=from_path, path2=to_path, recursive=recursive)

    def rm(self, path: str):
        """
        Deletes files in a path.

        Args:
            path (str): Path to a file or folder to be removed. If the path refers to
                a folder, it will be removed recursively.
        """

        wr.s3.delete_objects(boto3_session=self.session, path=path)

    def from_df(
        self,
        df: pd.DataFrame,
        path: str,
        **kwargs,
    ):
        """
        Upload a pandas `DataFrame` to a csv or parquet file. You can choose different
            file backends, and have the option of compression.

        Args:
            df (pd.DataFrame): Pandas DataFrame.
            path (str): Path to a S3 folder.
        """

        if path.endswith(".csv"):
            wr.s3.to_csv(
                boto3_session=self.session,
                df=df,
                path=path,
                dataset=True,
                **kwargs,
            )
        elif path.endswith(".parquet"):
            wr.s3.to_parquet(
                boto3_session=self.session,
                df=df,
                path=path,
                dataset=True,
                **kwargs,
            )
        else:
            raise ValueError("Only CSV and parquet formats are supported.")

    def to_df(
        self,
        path: str,
        **kwargs,
    ):
        """
        Reads a csv or parquet file to a pd.DataFrame.

        Args:
            path (str): Path to a S3 folder.
        """
        if path.endswith(".csv"):
            df = wr.s3.read_csv(
                boto3_session=self.session, path=path, dataset=True, **kwargs
            )
        elif path.endswith(".parquet"):
            df = wr.s3.read_parquet(
                boto3_session=self.session, path=path, dataset=True, **kwargs
            )
        else:
            raise ValueError("Only CSV and parquet formats are supported.")
        return df
