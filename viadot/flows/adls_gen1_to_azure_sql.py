from typing import Any, Dict, List

from prefect import Flow
from prefect.utilities import logging

from ..tasks import AzureDataLakeDownload, BlobFromCSV, CreateTableFromBlob

gen1_download_task = AzureDataLakeDownload(gen=1)
csv_to_blob_storage_task = BlobFromCSV()
blob_to_azure_sql_task = CreateTableFromBlob()


logger = logging.get_logger(__name__)


class ADLSGen1ToAzureSQL(Flow):
    """Bulk insert a file from an Azure Data Lake gen1 to Azure SQL Database.

    Args:
        name (str): The name of the flow.
        path (str): The path to the Data Lake file/folder.
        blob_path (str): The path of the generated blob.
        dtypes (dict): Which dtypes to use when creating the table in Azure SQL Database.
        local_file_path (str): Where the gen1 file should be downloaded.
        sp_credentials_secret (str): The Key Vault secret holding Service Pricipal credentials
        vault_name (str): The name of the vault from which to retrieve `sp_credentials_secret`
    """

    def __init__(
        self,
        name: str,
        path: str,
        blob_path: str,
        local_file_path: str = None,
        dtypes: Dict[str, Any] = None,
        overwrite_blob: bool = True,
        sep: str = "\t",
        table: str = None,
        schema: str = None,
        if_exists: str = "replace",
        sp_credentials_secret: str = None,
        vault_name: str = None,
        *args: List[any],
        **kwargs: Dict[str, Any]
    ):

        self.path = path
        self.local_file_path = local_file_path or self.slugify(name) + ".csv"
        self.blob_path = blob_path
        self.overwrite_blob = overwrite_blob
        self.sep = sep
        self.table = table
        self.schema = schema
        self.dtypes = dtypes
        self.if_exists = if_exists
        self.sp_credentials_secret = sp_credentials_secret
        self.vault_name = vault_name
        super().__init__(*args, name=name, **kwargs)
        self.gen_flow()

    @staticmethod
    def slugify(name):
        return name.replace(" ", "_").lower()

    def gen_flow(self) -> Flow:
        gen1_download_task.bind(
            from_path=self.path,
            to_path=self.local_file_path,
            gen=1,
            sp_credentials_secret=self.sp_credentials_secret,
            vault_name=self.vault_name,
            flow=self,
        )
        csv_to_blob_storage_task.bind(
            from_path=self.local_file_path,
            to_path=self.blob_path,
            overwrite=self.overwrite_blob,
            flow=self,
        )
        blob_to_azure_sql_task.bind(
            blob_path=self.blob_path,
            schema=self.schema,
            table=self.table,
            dtypes=self.dtypes,
            sep=self.sep,
            if_exists=self.if_exists,
            flow=self,
        )

        csv_to_blob_storage_task.set_upstream(gen1_download_task, flow=self)
        blob_to_azure_sql_task.set_upstream(csv_to_blob_storage_task, flow=self)
