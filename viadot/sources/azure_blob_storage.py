from typing import Any, Dict

from azure.storage.blob import BlobClient, BlobServiceClient

from ..config import local_config
from .base import Source


class AzureBlobStorage(Source):
    """
    A class for pulling data from the Azure Blob Storage
    (do not confuse with Azure Data Lake Gen1 and Azure Data Lake Gen2).

    Documentation for this API is located at: https://supermetrics.com/docs/product-api-getting-started/

    Parameters
    ----------
    credentials : Dict[str, Any], optional
        Credentials containing a connection string, by default None
    """

    def __init__(self, credentials: Dict[str, Any] = None, *args, **kwargs):
        DEFAULT_CREDENTIALS = local_config.get("AZURE_BLOB_STORAGE")
        credentials = credentials or DEFAULT_CREDENTIALS
        super().__init__(*args, credentials=credentials, **kwargs)

    def to_storage(self, from_path: str, to_path: str, overwrite: bool = False):
        """Upload a file to the storage.

        Args:
            from_path (str): Path to the local file to be uploaded to the storge.
            to_path (str): The destination path in the format 'container/path/a.csv'
            overwrite (bool, optional): [description]. Defaults to False.

        Example:
        ```python
        from viadot.sources import AzureBlobStorage
        storage = AzureBlobStorage()
        storage.to_storage('tests/test.csv')
        ```

        Returns:
            bool: Whether the operation was successful.
        """
        conn_str = self.credentials["CONNECTION_STRING"]
        blob_service_client = BlobServiceClient.from_connection_string(conn_str)

        container_name = to_path.split("/")[0]
        blob_path = "/".join(to_path.split("/")[1:])
        blob_client = blob_service_client.get_blob_client(
            container=container_name, blob=blob_path
        )

        with open(from_path, "rb") as data:
            blob_client.upload_blob(data, overwrite=overwrite)

        return True

    def exists(self, path: str) -> bool:
        """
        Check if blob exists in Azure Storage.

        Args:
            path (str): The bolb path in the format 'container/path/a.csv'

        Example:
        ```python
        from viadot.sources.azure_blob_storage import AzureBlobStorage

        path = "tests/test.csv"
        container_name = path.split("/")[0]
        blob_path = "/".join(path.split("/")[1:])
        conn_str = self.credentials["CONNECTION_STRING"]
        blob = BlobClient.from_connection_string(
            conn_str=conn_str, container_name=container_name, blob_name=blob_path
        )

        blob.exists()
        ```

        Returns:
            bool: Whether the operation was successful.
        """
        container_name = path.split("/")[0]
        blob_path = "/".join(path.split("/")[1:])
        conn_str = self.credentials["CONNECTION_STRING"]

        blob = BlobClient.from_connection_string(
            conn_str=conn_str, container_name=container_name, blob_name=blob_path
        )
        if_exists = blob.exists()
        return if_exists
