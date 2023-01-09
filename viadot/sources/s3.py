from typing import List

import awswrangler as wr
import boto3
import pandas as pd

from viadot.sources.base import Source


class S3(Source):
    """
    A class for pulling data from and uploading to S3.

    Args:
        profile_name (str, optional): The name of the AWS profile.
        aws_secret_access_key (str, optional): AWS secret access key
        aws_session_token (str, optional): AWS temporary session token
    """

    def __init__(
        self,
        profile_name: str = None,
        aws_access_key_id: str = None,
        aws_secret_access_key: str = None,
    ):
        if profile_name:
            self.session = boto3.session.Session(profile_name=profile_name)
        elif aws_access_key_id and aws_secret_access_key:
            self.session = boto3.session.Session(
                aws_access_key_id=aws_access_key_id,
                aws_secret_access_key=aws_secret_access_key,
            )
        else:
            self.session = boto3.session.Session()

    def ls(self, path: str, suffix: str = None) -> List[str]:
        """
        Returns a list of files in a S3.

        Args:
            path (str): Path to a folder.
            suffix (Union[str, List[str], None]) - Suffix or List of suffixes for
                filtering S3 keys.
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

    def cp(self, paths: List[str], from_path: str, to_path: str):
        """
        Copies the contents of `from_path` to `to_path`.

        Args:
            paths (List[str]): List of S3 objects paths,
                e.g. [s3://bucket/dir0/key0, s3://bucket/dir0/key1].
            from_path (str, optional): S3 Path for the source directory.
            to_path (str, optional): S3 Path for the target directory.
        """

        wr.s3.copy_objects(
            boto3_session=self.session,
            paths=paths,
            source_path=from_path,
            target_path=to_path,
        )

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
        df: pd.DataFrame = None,
        path: str = None,
        **kwargs,
    ):
        """
        Upload a pandas `DataFrame` to a csv or parquet file. You can choose different
            file backends, and have the option of compression.

        Args:
            df (pd.DataFrame, optional): Pandas DataFrame
            path (str, optional): Path to a S3 folder. Defaults to None.
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
        path: str = None,
        **kwargs,
    ):
        """
        Reads a csv or parquet file to a pd.DataFrame.

        Args:
            path (str, optional): Path to a S3 folder. Defaults to None.
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
