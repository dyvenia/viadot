from datetime import timezone, datetime
from typing import Any, Dict, List

import pandas as pd
from prefect import Flow, task
from prefect.utilities import logging

from ..tasks import (
    AzureDataLakeDownload,
    AzureDataLakeUpload,
    AzureSQLCreateTable,
    BCPTask,
)

gen1_download_task = AzureDataLakeDownload(gen=1)
gen2_upload_task = AzureDataLakeUpload(gen=2)
create_table_task = AzureSQLCreateTable()
bulk_insert_task = BCPTask()


logger = logging.get_logger(__name__)

METADATA_COLUMNS = {"_viadot_downloaded_at_utc": "DATETIME"}


@task
def add_ingestion_metadata(
    path: str,
    sep: str = "\t",
):
    """Add ingestion metadata column(s), eg. data download date"""
    df = pd.read_csv(path, sep=sep)
    df["_viadot_downloaded_at_utc"] = datetime.now(timezone.utc).replace(microsecond=0)
    df.to_csv(path, sep=sep, index=False)


class ADLSGen1ToAzureSQLNew(Flow):
    """Move file(s) from Azure Data Lake gen1 to gen2.

    Args:
        name (str): The name of the flow.
        gen1_path (str): The path to the gen1 Data Lake file/folder.
        gen2_path (str): The path of the final gen2 file/folder.
        local_file_path (str): Where the gen1 file should be downloaded.
        overwrite (str): Whether to overwrite the destination file(s).
        gen1_sp_credentials_secret (str): The Key Vault secret holding Service Pricipal credentials for gen1 lake
        gen2_sp_credentials_secret (str): The Key Vault secret holding Service Pricipal credentials for gen2 lake
        sqldb_credentials_secret (str): The Key Vault secret holding Azure SQL Database credentials
        vault_name (str): The name of the vault from which to retrieve `sp_credentials_secret`
    """

    def __init__(
        self,
        name: str,
        gen1_path: str,
        gen2_path: str,
        local_file_path: str = None,
        overwrite: bool = True,
        sep: str = "\t",
        schema: str = None,
        table: str = None,
        dtypes: dict = None,
        if_exists: str = "fail",
        gen1_sp_credentials_secret: str = None,
        gen2_sp_credentials_secret: str = None,
        sqldb_credentials_secret: str = None,
        vault_name: str = None,
        *args: List[any],
        **kwargs: Dict[str, Any]
    ):

        self.gen1_path = gen1_path
        self.local_file_path = local_file_path or self.slugify(name) + ".csv"
        self.gen2_path = gen2_path
        self.overwrite = overwrite
        self.sep = sep
        self.schema = schema
        self.table = table
        self.dtypes = dtypes
        self.if_exists = if_exists
        self.gen1_sp_credentials_secret = gen1_sp_credentials_secret
        self.gen2_sp_credentials_secret = gen2_sp_credentials_secret
        self.sqldb_credentials_secret = sqldb_credentials_secret
        self.vault_name = vault_name
        super().__init__(*args, name=name, **kwargs)
        self.dtypes.update(METADATA_COLUMNS)
        self.gen_flow()

    @staticmethod
    def slugify(name):
        return name.replace(" ", "_").lower()

    def gen_flow(self) -> Flow:
        gen1_download_task.bind(
            from_path=self.gen1_path,
            to_path=self.local_file_path,
            gen=1,
            sp_credentials_secret=self.gen1_sp_credentials_secret,
            vault_name=self.vault_name,
            flow=self,
        )
        add_ingestion_metadata.bind(path=self.local_file_path, sep=self.sep, flow=self)
        gen2_upload_task.bind(
            from_path=self.local_file_path,
            to_path=self.gen2_path,
            overwrite=self.overwrite,
            sp_credentials_secret=self.gen2_sp_credentials_secret,
            vault_name=self.vault_name,
            flow=self,
        )
        create_table_task.bind(
            schema=self.schema,
            table=self.table,
            dtypes=self.dtypes,
            if_exists=self.if_exists,
            credentials_secret=self.sqldb_credentials_secret,
            vault_name=self.vault_name,
            flow=self,
        )
        bulk_insert_task.bind(
            path=self.local_file_path,
            schema=self.schema,
            table=self.table,
            credentials_secret=self.sqldb_credentials_secret,
            vault_name=self.vault_name,
            flow=self,
        )

        add_ingestion_metadata.set_upstream(gen1_download_task, flow=self)
        gen2_upload_task.set_upstream(add_ingestion_metadata, flow=self)
        create_table_task.set_upstream(add_ingestion_metadata, flow=self)
        bulk_insert_task.set_upstream(create_table_task, flow=self)
