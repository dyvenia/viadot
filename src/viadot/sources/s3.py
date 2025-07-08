"""A module for working with Amazon S3 as a data source."""

from collections.abc import Iterable, Iterator
from datetime import datetime
import io
import os
from pathlib import Path
from typing import Any, Literal

from boto3.s3.transfer import TransferConfig


try:
    import awswrangler as wr
    import boto3
    import s3fs
except ModuleNotFoundError:
    msg = "Missing required modules to use RedshiftSpectrum source."
    raise ImportError(msg) from None

import pandas as pd
from pydantic import BaseModel, root_validator

from viadot.config import get_source_credentials
from viadot.exceptions import CredentialError
from viadot.sources.base import Source


class S3Credentials(BaseModel):
    region_name: str  # The name of the AWS region.
    aws_access_key_id: str  # The AWS access key ID.
    aws_secret_access_key: str  # The AWS secret access key.
    profile_name: str | None = None  # The name of the IAM profile to use.

    @root_validator(pre=True)
    def is_configured(cls, credentials: dict) -> dict:  # noqa: N805
        """Validate credentials.

        Ensure that at least one of the
        following is provided:
        - profile_name and region_name
        - aws_access_key_id, aws_secret_access_key, and region_name
        """
        profile_name = credentials.get("profile_name")
        region_name = credentials.get("region_name")
        aws_access_key_id = credentials.get("aws_access_key_id")
        aws_secret_access_key = credentials.get("aws_secret_access_key")

        profile_credential = profile_name and region_name
        direct_credential = aws_access_key_id and aws_secret_access_key and region_name

        if not (profile_credential or direct_credential):
            msg = "Either `profile_name` and `region_name`, or `aws_access_key_id`, "
            msg += "`aws_secret_access_key`, and `region_name` must be specified."
            raise CredentialError(msg)
        return credentials


class S3(Source):
    def __init__(
        self,
        credentials: S3Credentials | None = None,
        config_key: str | None = None,
        *args,
        **kwargs,
    ):
        """A class for pulling data from and uploading to the Amazon S3.

        Args:
        credentials (S3Credentials, optional): Amazon S3 credentials.
            Defaults to None.
        config_key (str, optional): The key in the viadot config holding relevant
            credentials. Defaults to None.
        """
        raw_creds = (
            credentials
            or get_source_credentials(config_key)
            or self._get_env_credentials()
        )
        validated_creds = dict(S3Credentials(**raw_creds))  # validate the credentials

        super().__init__(*args, credentials=validated_creds, **kwargs)

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

    def _get_env_credentials(self):
        return {
            "region_name": os.environ.get("AWS_DEFAULT_REGION"),
            "aws_access_key_id": os.environ.get("AWS_ACCESS_KEY_ID"),
            "aws_secret_access_key": os.environ.get("AWS_SECRET_ACCESS_KEY"),
        }

    def ls(self, path: str, suffix: str | None = None) -> list[str]:
        """Returns a list of objects in a provided path.

        Args:
            path (str): Path to a folder.
            suffix (Union[str, List[str], None]): Suffix or list of suffixes for
                filtering Amazon S3 keys. Defaults to None.
        """
        return wr.s3.list_objects(boto3_session=self.session, path=path, suffix=suffix)

    def exists(self, path: str) -> bool:
        """Check if an object exists in the Amazon S3.

        Args:
            path (str): The path to an object to check.

        Returns:
            bool: Whether the object exists.
        """
        if not path.startswith("s3://"):
            msg = "Path must be an AWS S3 URL ('s3://my/path')."
            raise ValueError(msg)

        # Note this only checks for files.
        file_exists = wr.s3.does_object_exist(boto3_session=self.session, path=path)

        if file_exists:
            return True

        # Use another method in case the path is a folder.
        client = self.session.client("s3")
        bucket = path.split("/")[2]
        path = str(Path(*path.rstrip("/").split("/")[3:]))

        response = client.list_objects_v2(Bucket=bucket, Prefix=path, Delimiter="/")

        folders_with_prefix: list[dict] = response.get("CommonPrefixes")
        if folders_with_prefix is None:
            folder_exists = False
        else:
            # This is because list_objects takes in `Prefix`, so eg. if there exists
            #  a path `a/b/abc` and we run `list_objects_v2(path=`a/b/a`)`,
            #  it would enlist `a/b/abc` as well.
            paths = [path["Prefix"].rstrip("/") for path in folders_with_prefix]
            folder_exists = path in paths
        return folder_exists

    def cp(self, from_path: str, to_path: str, recursive: bool = False) -> None:
        """Copies the contents of `from_path` to `to_path`.

        Args:
            from_path (str): The path (S3 URL) of the source directory.
            to_path (str): The path (S3 URL) of the target directory.
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

    def rm(self, path: str | list[str]) -> None:
        """Delete files under `path`.

        Args:
            path (list[str]): Path to a list of files or a directory
                to be removed. If the path is a directory, it will
                be removed recursively.

        Example:
        ```python
        from viadot.sources import S3

        s3 = S3()
        s3.rm(path=["file1.parquet"])
        ```
        """
        wr.s3.delete_objects(boto3_session=self.session, path=path)

    def from_df(
        self,
        df: pd.DataFrame,
        path: str,
        extension: Literal[".csv", ".parquet"] = ".parquet",
        **kwargs,
    ) -> None:
        """Upload a pandas `DataFrame` into Amazon S3 as a CSV or Parquet file.

        For a full list of available parameters, please refer to the official
        documentation:
        https://aws-sdk-pandas.readthedocs.io/en/3.0.0/stubs/awswrangler.s3.to_parquet.html
        https://aws-sdk-pandas.readthedocs.io/en/3.0.0/stubs/awswrangler.s3.to_csv.html

        Args:
            df (pd.DataFrame): The pandas DataFrame to upload.
            path (str): The destination path.
            extension (Literal[".csv", ".parquet"], optional): The file extension.
                Defaults to ".parquet".
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
            msg = "Only CSV and Parquet formats are supported."
            raise ValueError(msg)

    def to_df(
        self,
        paths: list[str],
        chunk_size: int | None = None,
        **kwargs,
    ) -> pd.DataFrame | Iterable[pd.DataFrame]:
        """Read a CSV or Parquet file into a pandas `DataFrame`.

        Args:
            paths (list[str]): A list of paths to Amazon S3 files. All files under the
                path must be of the same type.
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
                boto3_session=self.session, path=paths, chunksize=chunk_size, **kwargs
            )
        elif paths[0].endswith(".parquet"):
            df = wr.s3.read_parquet(
                boto3_session=self.session, path=paths, chunked=chunk_size, **kwargs
            )
        else:
            msg = "Only CSV and Parquet formats are supported."
            raise ValueError(msg)
        return df

    def upload(self, from_path: str, to_path: str) -> None:
        """Upload file(s) to S3.

        Args:
            from_path (str): Path to local file(s) to be uploaded.
            to_path (str): Path to the destination file/folder.
        """
        wr.s3.upload(boto3_session=self.session, local_file=from_path, path=to_path)

    def download(self, from_path: str, to_path: str) -> None:
        """Download file(s) from Amazon S3.

        Args:
            from_path (str): Path to file in Amazon S3.
            to_path (str): Path to local file(s) to be stored.
        """
        wr.s3.download(boto3_session=self.session, path=from_path, local_file=to_path)

    def get_page_iterator(
        self,
        bucket_name: str,
        directory_path: str,
        operation_name: str = "list_objects_v2",
        **kwargs,
    ) -> Iterator[dict[str, Any]]:
        """Returns an iterator to paginate through the objects in S3 bucket directory.

        This method uses the S3 paginator to list objects under a specified directory
        path in a given S3 bucket. It can accept additional optional parameters
        through **kwargs, which will be passed to the paginator.

        Args:
            bucket_name (str): The name of the S3 bucket.
            directory_path (str): The directory path (prefix) in the bucket to list
                objects from.
            operation_name (str): The operation name. This is the same name as
                the method name on the client. Defaults as "list_objects_v2".
            **kwargs: Additional arguments to pass to the paginator (optional).

        Returns:
            Iterator: An iterator to paginate through the S3 objects.
        """
        client = self.session.client("s3")
        paginator = client.get_paginator(operation_name=operation_name)

        return paginator.paginate(Bucket=bucket_name, Prefix=directory_path, **kwargs)

    def get_object_sizes(self, file_paths: str | list[str]) -> dict[str, int | None]:
        """Retrieve the sizes of specified S3 objects.

        Args:
            file_paths (str | list[str]): A single file path or a list of file paths
                in S3 bucket.

        Returns:
            dict[str, int]: A dictionary where the keys are file paths and the values
                are their corresponding sizes in bytes.
        """
        return wr.s3.size_objects(boto3_session=self.session, path=file_paths)

    def describe_objects(
        self,
        file_paths: str | list[str],
        last_modified_begin: datetime | None = None,
        last_modified_end: datetime | None = None,
    ) -> dict[str, dict[str, Any]]:
        """Describe S3 objects from a received S3 prefix or list of S3 objects paths.

        Args:
            file_paths (str | list[str]): S3 prefix (accepts Unix shell-style wildcards)
                (e.g. s3://bucket/prefix) or list of S3 objects paths
                (e.g. [s3://bucket/key0, s3://bucket/key1]).
            last_modified_begin (datetime | None, optional): Filter the S3 files by
                the Last modified date of the object. The filter is applied only after
                list all s3 files. Defaults to None.
            last_modified_end (datetime | None, optional): Filter the s3 files by
                the Last modified date of the object. The filter is applied only after
                list all s3 files. Defaults to None.

        Returns:
            dict[str, dict[str, Any]]: Return a dictionary of objects returned from
            head_objects where the key is the object path.

        Examples:
            >>> s3 = S3(credentials=aws_credentials)
                # Describe both objects
            >>> descs0 = s3.describe_objects(['s3://bucket/key0', 's3://bucket/key1'])
                # Describe all objects under the prefix
            >>> descs1 = s3.describe_objects('s3://bucket/prefix')
        """
        return wr.s3.describe_objects(
            boto3_session=self.session,
            path=file_paths,
            last_modified_begin=last_modified_begin,
            last_modified_end=last_modified_end,
        )

    def upload_file_object(
        self,
        file_object: io.BytesIO,
        bucket_name: str,
        path: str,
        config: dict | None = None,
    ) -> None:
        """Upload a file-like object to S3.

        The file-like object must be in binary mode.
        This is a managed transfer which will perform a multipart upload in multiple
        threads if necessary.

        Args:
            file_object (io.BytesIO): A file-like object to upload.
            bucket_name (str): The name of the bucket to upload to.
            path (str): The name of the key to upload to.
            config (dict, optional): Configuration for multipart upload.
                If None, a default config with a 25MB multipart threshold and 10 threads
                is used.
        """
        if config is None:
            config = TransferConfig(
                multipart_threshold=25 * 1024 * 1024,  # 25 MB
                multipart_chunksize=25 * 1024 * 1024,  # 25 MB
                max_concurrency=10,
                use_threads=True,
            )
        else:
            config = TransferConfig(**config)

        client = self.session.client("s3")

        if not hasattr(file_object, "read"):
            msg = "file_object must be a file-like object with a read() method."
            raise ValueError(msg)

        client.upload_fileobj(
            Fileobj=file_object, Bucket=bucket_name, Key=path, Config=config
        )
