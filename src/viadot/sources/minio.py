"""A module for interacting with MinIO."""

from collections.abc import Generator
from pathlib import Path
from typing import Literal

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq


try:
    from minio import Minio
    from minio.error import S3Error
    import s3fs
except ModuleNotFoundError as e:
    msg = "Missing required modules to use MinIO source."
    raise ImportError(msg) from e

from pydantic import BaseModel
import urllib3
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
    def __init__(
        self,
        credentials: MinIOCredentials | None = None,
        config_key: str | None = None,
        *args,
        **kwargs,
    ):
        """A class for interacting with MinIO.

        Interact with MinIO in a more Pythonic, user-friendly, and robust way than the
        official minio client.

        Args:
        credentials (MinIOCredentials): MinIO credentials.
        config_key (str, optional): The key in the viadot config holding relevant
            credentials.
        """
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
        basename_template: str | None = None,
        partition_cols: list[str] | None = None,
        if_exists: Literal["error", "delete_matching", "overwrite_or_ignore"] = "error",
    ) -> None:
        """Create a Parquet dataset on MinIO from a PyArrow Table.

        Uses multi-part upload to upload the table in chunks, speeding up the
        process by using multithreading and avoiding upload size limits.

        Either both `schema_name` and `table_name` or only `path` must be provided.

        `path` allows specifying an arbitrary path, while `schema_name` and `table_name`
        provide a shortcut for creating a data lakehouse-like structure of
        `s3://<bucket>/<schema_name>/<table_name>/<table_name>.parquet`.

        For more information on partitioning, see
        https://arrow.apache.org/docs/python/generated/pyarrow.parquet.write_to_dataset.html#pyarrow-parquet-write-to-dataset

        Args:
            table (pa.Table): The table to upload.
            schema_name (str, optional): The name of the schema directory. Defaults to
                None.
            table_name (str, optional): The name of the table directory. Defaults to
                None.
            path (str | Path, optional): The path to the destination file. Defaults to
                None.
            basename_template (str, optional): A template string used to generate
                basenames of written data files. The token '{i}' will be replaced with
                an automatically incremented integer. Defaults to None.
            partition_cols (list[str], optional): The columns to partition by. Defaults
                to None.
            if_exists (Literal["error", "delete_matching", "overwrite_or_ignore"],
                optional): What to do if the dataset already exists.
        """
        fqn_or_path = (schema_name and table_name) or (
            path and not (schema_name or table_name)
        )
        if not fqn_or_path:
            msg = "Either both `schema_name` and `table_name` or only `path` must be provided."
            raise ValueError(msg)

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
            basename_template=basename_template,
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
        basename_template: str | None = None,
        partition_cols: list[str] | None = None,
        if_exists: Literal["error", "delete_matching", "overwrite_or_ignore"] = "error",
    ) -> None:
        """Create a Parquet dataset on MinIO from a PyArrow Table.

        Uses multi-part upload to upload the table in chunks, speeding up the
        process by using multithreading and avoiding upload size limits.

        Either both `schema_name` and `table_name` or only `path` must be provided.

        `path` allows specifying an arbitrary path, while `schema_name` and `table_name`
        provide a shortcut for creating a data lakehouse-like structure of
        `s3://<bucket>/<schema_name>/<table_name>/<table_name>.parquet`.

        For more information on partitioning, see
        https://arrow.apache.org/docs/python/generated/pyarrow.parquet.write_to_dataset.html#pyarrow-parquet-write-to-dataset

        Args:
            df (pd.DataFrame): The DataFrame to upload.
            schema_name (str, optional): The name of the schema directory. Defaults to
                None.
            table_name (str, optional): The name of the table directory. Defaults to
                None.
            path (str | Path, optional): The path to the destination file. Defaults to
                None.
            basename_template (str, optional): A template string used to generate
                basenames of written data files. The token '{i}' will be replaced with
                an automatically incremented integer. Defaults to None.
            partition_cols (list[str], optional): The columns to partition by. Defaults
                to None.
            if_exists (Literal["error", "delete_matching", "overwrite_or_ignore"],
                optional): What to do if the dataset already exists.
        """
        table = pa.Table.from_pandas(df)

        return self.from_arrow(
            table=table,
            schema_name=schema_name,
            table_name=table_name,
            path=path,
            basename_template=basename_template,
            partition_cols=partition_cols,
            if_exists=if_exists,
        )

    def ls(self, path: str) -> Generator[str, None, None]:
        """List files and directories under `path`.

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
        """Remove a file or directory from MinIO.

        Args:
            path (str): The path to the file to remove.
        """
        if recursive:
            bucket = path.split("/")[2]
            prefix = "/".join(path.split("/")[3:])
            objects = self.client.list_objects(bucket, prefix=prefix, recursive=True)
            for obj in objects:
                self.client.remove_object(bucket, obj.object_name)
            return

        self.client.remove_object(self.bucket, path)

    def _check_if_file_exists(self, path: str) -> bool:
        try:
            self.client.stat_object(self.bucket, path)
        except S3Error as e:
            if "Object does not exist" in e.message:
                return False
            raise
        else:
            return True

    def check_connection(self) -> None:
        """Verify connectivity to the MinIO endpoint."""
        try:
            self.client.bucket_exists(self.bucket)
        except NewConnectionError as e:
            msg = f"Connection to MinIO endpoint '{self.endpoint}' failed with error: \n{e}"
            msg += "Please check your credentials and try again."

            raise ValueError(msg) from e
        except Exception as e:
            msg = f"Connection to MinIO endpoint '{self.endpoint}' failed with error: \n{e}"
            raise ValueError(msg) from e
        self.logger.info("Connection successful!")
