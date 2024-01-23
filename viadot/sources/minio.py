from pathlib import Path
from typing import Generator, Literal

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import s3fs
import urllib3
from minio import Minio
from minio.error import S3Error
from pydantic import BaseModel
from urllib3.exceptions import NewConnectionError

from viadot.config import get_source_credentials
from viadot.sources.base import Source


class MinIOCredentials(BaseModel):
    endpoint: str = "localhost:9000"
    access_key: str
    secret_key: str
    bucket: str
    secure: bool = True  # Whether to use HTTPS.
    verify: bool = True  # Whether to verify TLS certificates.


class MinIO(Source):
    """
    A class for interacting with MinIO, in a more Pythonic, user-friendly, and robust
    way than the official minio client.

    Args:
        credentials (MinIOCredentials): MinIO credentials.
        config_key (str, optional): The key in the viadot config holding relevant credentials.
    """

    def __init__(
        self,
        credentials: MinIOCredentials = None,
        config_key: str = None,
        *args,
        **kwargs,
    ):
        raw_creds = credentials or get_source_credentials(config_key) or {}
        validated_creds = MinIOCredentials(**raw_creds).dict(
            by_alias=True
        )  # validate the credentials

        super().__init__(*args, credentials=validated_creds, **kwargs)

        self.endpoint = self.credentials.get("endpoint")
        self.access_key = self.credentials.get("access_key")
        self.secret_key = self.credentials.get("secret_key")
        self.bucket = self.credentials.get("bucket")
        self.secure = self.credentials.get("secure")
        self.verify = self.credentials.get("verify")

        self.http_scheme = "https" if self.secure is True else "http"

        self.client = Minio(
            self.endpoint,
            access_key=self.access_key,
            secret_key=self.secret_key,
            secure=self.secure,
            http_client=urllib3.PoolManager(
                timeout=urllib3.Timeout.DEFAULT_TIMEOUT,
                retries=urllib3.Retry(
                    connect=1,
                    read=3,
                    total=2,
                    backoff_factor=1,
                    status_forcelist=[500, 502, 503, 504],
                ),
                cert_reqs="CERT_REQUIRED" if self.verify else "CERT_NONE",
            ),
        )

        self.storage_options = {
            "key": self.access_key,
            "secret": self.secret_key,
            "client_kwargs": {
                "endpoint_url": f"{self.http_scheme}://" + self.endpoint,
                "use_ssl": self.secure,
                "verify": self.verify,
            },
        }

        self.fs = s3fs.S3FileSystem(
            key=self.storage_options["key"],
            secret=self.storage_options["secret"],
            client_kwargs=self.storage_options["client_kwargs"],
            anon=False,
        )

    def from_arrow(
        self,
        table: pa.Table,
        schema_name: str | None = None,
        table_name: str | None = None,
        path: str | Path | None = None,
        partition_cols: list[str] | None = None,
        if_exists: Literal["error", "delete_matching", "overwrite_or_ignore"] = "error",
    ):
        """
        Create a Parquet dataset on MinIO from a PyArrow Table.

        Uses multi-part upload to upload the table in chunks, speeding up the
        process by using multithreading and avoiding upload size limits.

        Either both `schema_name` and `table_name` or only `path` must be provided.

        `path` allows specifying an arbitrary path, while `schema_name` and `table_name`
        provide a shortcut for creating a data lakehouse-like structure of
        `s3://<bucket>/<schema_name>/<table_name>/<table_name>.parquet`.

        For more information on partitioning, see
        https://arrow.apache.org/docs/python/generated/pyarrow.parquet.write_to_dataset.html#pyarrow-parquet-write-to-dataset  # noqa

        Args:
            table (pa.Table): The table to upload.
            schema_name (str, optional): The name of the schema directory. Defaults to
                None.
            table_name (str, optional): The name of the table directory. Defaults to
                None.
            path (str | Path, optional): The path to the destination file. Defaults to
                None.
            partition_cols (list[str], optional): The columns to partition by. Defaults
                to None.
            if_exists (Literal["error", "delete_matching", "overwrite_or_ignore"],
                optional). What to do if the dataset already exists.
        """
        fqn_or_path = (schema_name and table_name) or (
            path and not (schema_name or table_name)
        )
        if not fqn_or_path:
            raise ValueError(
                "Either both `schema_name` and `table_name` or only `path` must be provided."
            )

        # We need to create the dirs here as PyArrow also tries to create the bucket,
        # which shouldn't be allowed for whomever is executing this code.
        self.logger.debug(f"Creating directory for table {table_name}...")
        path = path or f"{schema_name}/{table_name}"
        self.fs.makedirs(path, exist_ok=True)
        self.logger.debug("Directory has been created successfully.")

        # Write the data.
        pq.write_to_dataset(
            table,
            root_path=path,
            partition_cols=partition_cols,
            existing_data_behavior=if_exists,
            filesystem=self.fs,
            max_rows_per_file=1024 * 1024,
            create_dir=False,  # Required as Arrow attempts to create the bucket, too.
        )

    def from_df(
        self,
        df: pd.DataFrame,
        schema_name: str | None = None,
        table_name: str | None = None,
        path: str | Path | None = None,
        partition_cols: list[str] | None = None,
        if_exists: Literal["error", "delete_matching", "overwrite_or_ignore"] = "error",
    ) -> None:
        """
        Create a Parquet dataset on MinIO from a PyArrow Table.

        Uses multi-part upload to upload the table in chunks, speeding up the
        process by using multithreading and avoiding upload size limits.

        Either both `schema_name` and `table_name` or only `path` must be provided.

        `path` allows specifying an arbitrary path, while `schema_name` and `table_name`
        provide a shortcut for creating a data lakehouse-like structure of
        `s3://<bucket>/<schema_name>/<table_name>/<table_name>.parquet`.

        For more information on partitioning, see
        https://arrow.apache.org/docs/python/generated/pyarrow.parquet.write_to_dataset.html#pyarrow-parquet-write-to-dataset  # noqa

        Args:
            df (pd.DataFrame): The DataFrame to upload.
            schema_name (str, optional): The name of the schema directory. Defaults to
                None.
            table_name (str, optional): The name of the table directory. Defaults to
                None.
            path (str | Path, optional): The path to the destination file. Defaults to
                None.
            partition_cols (list[str], optional): The columns to partition by. Defaults
                to None.
            if_exists (Literal["error", "delete_matching", "overwrite_or_ignore"],
                optional). What to do if the dataset already exists.
        """
        table = pa.Table.from_pandas(df)

        return self.from_arrow(
            table=table,
            schema_name=schema_name,
            table_name=table_name,
            path=path,
            partition_cols=partition_cols,
            if_exists=if_exists,
        )

    def ls(self, path: str) -> Generator[str, None, None]:
        """
        List files and directories under `path`.

        List operation can be slow if there are a lot of objects, hence using a
        generator.

        Args:
            path (str): The path which contents should be listed.

        Yields:
            Generator[str, None, None]: Contents (files and directories) of `path`.
        """
        for obj in self.client.list_objects(self.bucket, path):
            yield obj.object_name

    def rm(self, path: str, recursive: bool = False) -> None:
        """
        Remove a file from MinIO. Recursive removal (directories) is not yet supported.

        Args:
            path (str): The path to the file to remove.
        """
        if recursive:
            # In order to do this, we need to recurse through all
            # subdirectories, list all objects in each, and delete them. Thankfully,
            # MinIO provides a `remove_objects()` method that simplifies the process.
            raise NotImplementedError

        self.client.remove_object(self.bucket, path)

    def _check_if_file_exists(self, path: str) -> bool:
        try:
            self.client.stat_object(self.bucket, path)
            return True
        except S3Error as e:
            if "Object does not exist" in e.message:
                return False
            else:
                raise e

    def check_connection(self) -> None:
        """Verify connectivity to the MinIO endpoint."""
        try:
            self.client.bucket_exists(self.bucket)
        except NewConnectionError as e:
            raise ValueError(
                f"Connection to MinIO endpoint '{self.endpoint}' failed with error: \n{e}",
                "Please check your credentials and try again.",
            )
        except Exception as e:
            raise ValueError(
                f"Connection to MinIO endpoint '{self.endpoint}' failed with error: \n{e}"
            )
        self.logger.info("Connection successful!")
