import os
from pathlib import Path
from typing import Any, Dict, List

import pendulum
from prefect import Flow
from prefect.backend import set_key_value
from prefect.utilities import logging

from viadot.task_utils import (
    add_ingestion_metadata_task,
    df_clean_column,
    df_get_data_types_task,
    df_map_mixed_dtypes_for_parquet,
    df_to_csv,
    df_to_parquet,
    dtypes_to_json_task,
    update_dtypes_dict,
)
from viadot.tasks import AzureDataLakeUpload, SalesforceToDF


logger = logging.get_logger(__name__)


class SalesforceToADLS(Flow):
    def __init__(
        self,
        name: str = None,
        query: str = None,
        table: str = None,
        columns: List[str] = None,
        domain: str = "test",
        client_id: str = "viadot",
        env: str = "DEV",
        vault_name: str = None,
        credentials_secret: str = None,
        output_file_extension: str = ".parquet",
        overwrite_adls: bool = True,
        adls_dir_path: str = None,
        local_file_path: str = None,
        adls_file_name: str = None,
        adls_sp_credentials_secret: str = None,
        if_exists: str = "replace",
        timeout: int = 3600,
        *args: List[Any],
        **kwargs: Dict[str, Any],
    ):
        """
        Flow for downloading data from Salesforce table to a local CSV or Parquet file, then uploading it to Azure Data Lake.

        Args:
            name (str, optional): Flow name. Defaults to None.
            query (str, optional): Query for download the data if specific download is needed. Defaults to None.
            table (str, optional): Table name. Can be used instead of query. Defaults to None.
            columns (List[str], optional): List of columns which are needed - table argument is needed. Defaults to None.
            domain (str, optional): Domain of a connection; defaults to 'test' (sandbox).
                Can only be added if built-in username/password/security token is provided. Defaults to None.
            client_id (str, optional): Client id to keep the track of API calls. Defaults to None.
            env (str, optional): Environment information, provides information about credential
                and connection configuration. Defaults to 'DEV'.
            credentials_secret (str, optional): The name of the Azure Key Vault secret for Salesforce. Defaults to None.
            vault_name (str, optional): The name of the vault from which to obtain the secrets. Defaults to None.
            output_file_extension (str, optional): Output file extension - to allow selection of CSV for data
                which is not easy to handle with parquet. Defaults to ".parquet".
            overwrite_adls (bool, optional): Whether to overwrite the file in ADLS. Defaults to True.
            adls_dir_path (str, optional): Azure Data Lake destination folder/catalog path. Defaults to None.
            local_file_path (str, optional): Local destination path. Defaults to None.
            adls_file_name (str, optional): Name of file in ADLS. Defaults to None.
            adls_sp_credentials_secret (str, optional): The name of the Azure Key Vault secret containing a dictionary with
                ACCOUNT_NAME and Service Principal credentials (TENANT_ID, CLIENT_ID, CLIENT_SECRET) for the Azure Data Lake.
                Defaults to None.
            if_exists (str, optional): What to do if the file exists. Defaults to "replace".
            timeout(int, optional): The amount of time (in seconds) to wait while running this task before
                a timeout occurs. Defaults to 3600.
        """
        # SalesforceToDF
        self.query = query
        self.table = table
        self.columns = columns
        self.domain = domain
        self.client_id = client_id
        self.env = env
        self.vault_name = vault_name
        self.credentials_secret = credentials_secret

        # AzureDataLakeUpload
        self.adls_sp_credentials_secret = adls_sp_credentials_secret
        self.if_exists = if_exists
        self.output_file_extension = output_file_extension
        self.now = str(pendulum.now("utc"))
        self.timeout = timeout

        self.local_file_path = (
            local_file_path or self.slugify(name) + self.output_file_extension
        )
        self.local_json_path = self.slugify(name) + ".json"
        self.adls_dir_path = adls_dir_path

        if adls_file_name is not None:
            self.adls_file_path = os.path.join(adls_dir_path, adls_file_name)
            self.adls_schema_file_dir_file = os.path.join(
                adls_dir_path, "schema", Path(adls_file_name).stem + ".json"
            )
        else:
            self.adls_file_path = os.path.join(
                adls_dir_path, self.now + self.output_file_extension
            )
            self.adls_schema_file_dir_file = os.path.join(
                adls_dir_path, "schema", self.now + ".json"
            )
        self.overwrite_adls = overwrite_adls

        super().__init__(*args, name=name, **kwargs)

        self.gen_flow()

    @staticmethod
    def slugify(name):
        return name.replace(" ", "_").lower()

    def gen_flow(self) -> Flow:
        salesforce_to_df_task = SalesforceToDF(timeout=self.timeout)
        df = salesforce_to_df_task.bind(
            query=self.query,
            table=self.table,
            columns=self.columns,
            domain=self.domain,
            client_id=self.client_id,
            env=self.env,
            vault_name=self.vault_name,
            credentials_secret=self.credentials_secret,
            flow=self,
        )

        df_clean = df_clean_column.bind(df=df, flow=self)
        df_with_metadata = add_ingestion_metadata_task.bind(df_clean, flow=self)
        dtypes_dict = df_get_data_types_task.bind(df_with_metadata, flow=self)
        df_to_be_loaded = df_map_mixed_dtypes_for_parquet(
            df_with_metadata, dtypes_dict, flow=self
        )

        if self.output_file_extension == ".parquet":
            df_to_file = df_to_parquet.bind(
                df=df_to_be_loaded,
                path=self.local_file_path,
                if_exists=self.if_exists,
                flow=self,
            )
        else:
            df_to_file = df_to_csv.bind(
                df=df_with_metadata,
                path=self.local_file_path,
                if_exists=self.if_exists,
                flow=self,
            )

        file_to_adls_task = AzureDataLakeUpload(timeout=self.timeout)
        file_to_adls_task.bind(
            from_path=self.local_file_path,
            to_path=self.adls_file_path,
            overwrite=self.overwrite_adls,
            sp_credentials_secret=self.adls_sp_credentials_secret,
            flow=self,
        )

        dtypes_updated = update_dtypes_dict(dtypes_dict, flow=self)
        dtypes_to_json_task.bind(
            dtypes_dict=dtypes_updated, local_json_path=self.local_json_path, flow=self
        )

        json_to_adls_task = AzureDataLakeUpload(timeout=self.timeout)
        json_to_adls_task.bind(
            from_path=self.local_json_path,
            to_path=self.adls_schema_file_dir_file,
            overwrite=self.overwrite_adls,
            sp_credentials_secret=self.adls_sp_credentials_secret,
            flow=self,
        )

        df_clean.set_upstream(df, flow=self)
        df_with_metadata.set_upstream(df_clean, flow=self)
        dtypes_dict.set_upstream(df_with_metadata, flow=self)
        df_to_be_loaded.set_upstream(dtypes_dict, flow=self)
        file_to_adls_task.set_upstream(df_to_file, flow=self)
        json_to_adls_task.set_upstream(dtypes_to_json_task, flow=self)
        set_key_value(key=self.adls_dir_path, value=self.adls_file_path)
