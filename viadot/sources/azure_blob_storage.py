from typing import Any, Dict

from azure.storage.blob import BlobServiceClient, ContentSettings

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

    def to_storage(self, from_path: str, to_path: str):

        conn_str = self.credentials["CONNECTION_STRING"]
        blob_service_client = BlobServiceClient.from_connection_string(conn_str)

        container_name = to_path.split("/")[0]
        blob_path = "/".join(to_path.split("/")[1:])
        blob_client = blob_service_client.get_blob_client(
            container=container_name, blob=blob_path
        )

        return True
