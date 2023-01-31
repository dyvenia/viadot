import os
from pathlib import Path
from typing import Any, Dict, List

import pendulum
from prefect import Flow
from prefect.backend import set_key_value
from prefect.utilities import logging

from viadot.task_utils import (
    add_ingestion_metadata_task,
    df_get_data_types_task,
    df_map_mixed_dtypes_for_parquet,
    df_to_csv,
    df_to_parquet,
    dtypes_to_json_task,
    update_dtypes_dict,
)
from viadot.tasks import AzureDataLakeUpload, BigQueryToDF


logger = logging.get_logger(__name__)


class BigQueryToADLS(Flow):
    def __init__(
        self,
        name: str = None,
        dataset_name: str = None,
        table_name: str = None,
        date_column_name: str = "date",
        start_date: str = None,
        end_date: str = None,
        credentials_key: str = "BIGQUERY",
        vault_name: str = None,
        credentials_secret: str = None,
        output_file_extension: str = ".parquet",
        adls_dir_path: str = None,
        local_file_path: str = None,
        adls_file_name: str = None,
        adls_sp_credentials_secret: str = None,
        overwrite_adls: bool = False,
        if_exists: str = "replace",
        timeout: int = 3600,
        *args: List[Any],
        **kwargs: Dict[str, Any],
    ):
        """
        Flow for downloading data from BigQuery project to a local CSV or Parquet file
        using Bigquery API, then uploading it to Azure Data Lake.

        There are 3 cases:
            If start_date and end_date are not None - all data from the start date to the end date will be retrieved.
            If start_date and end_date are left as default (None) - the data is pulled till "yesterday" (current date -1)
            If the column that looks like a date does not exist in the table, get all the data from the table.

        Args:
            name (str): The name of the flow.
            dataset_name (str, optional): Dataset name. Defaults to None.
            table_name (str, optional): Table name. Defaults to None.
            date_column_name (str, optional): The query is based on a date, the user can provide the name
            of the date columnn if it is different than "date". If the user-specified column does not exist,
            all data will be retrieved from the table. Defaults to "date".
            start_date (str, optional): A query parameter to pass start date e.g. "2022-01-01". Defaults to None.
            end_date (str, optional): A query parameter to pass end date e.g. "2022-01-01". Defaults to None.
            credentials_key (str, optional): Credential key to dictionary where details are stored (local config).
            credentials can be generated as key for User Principal inside a BigQuery project. Defaults to "BIGQUERY".
            credentials_secret (str, optional): The name of the Azure Key Vault secret for Bigquery project. Defaults to None.
            vault_name (str, optional): The name of the vault from which to obtain the secrets. Defaults to None.
            output_file_extension (str, optional): Output file extension - to allow selection of.csv for data
            which is not easy to handle with parquet. Defaults to ".parquet".
            adls_dir_path (str, optional): Azure Data Lake destination folder/catalog path. Defaults to None.
            local_file_path (str, optional): Local destination path. Defaults to None.
            adls_file_name (str, optional): Name of file in ADLS. Defaults to None.
            adls_sp_credentials_secret (str, optional): The name of the Azure Key Vault secret containing a dictionary with
            ACCOUNT_NAME and Service Principal credentials (TENANT_ID, CLIENT_ID, CLIENT_SECRET) for the Azure Data Lake.
            Defaults to None.
            overwrite_adls (bool, optional): Whether to overwrite files in the lake. Defaults to False.
            if_exists (str, optional): What to do if the file exists. Defaults to "replace".
            timeout(int, optional): The amount of time (in seconds) to wait while running this task before
                a timeout occurs. Defaults to 3600.
        """
        # BigQueryToDF
        self.credentials_key = credentials_key
        self.dataset_name = dataset_name
        self.table_name = table_name
        self.start_date = start_date
        self.end_date = end_date
        self.date_column_name = date_column_name
        self.vault_name = vault_name
        self.credentials_secret = credentials_secret

        # AzureDataLakeUpload
        self.overwrite = overwrite_adls
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

        super().__init__(*args, name=name, **kwargs)

        self.gen_flow()

    @staticmethod
    def slugify(name):
        return name.replace(" ", "_").lower()

    def gen_flow(self) -> Flow:
        bigquery_to_df_task = BigQueryToDF(timeout=self.timeout)
        df = bigquery_to_df_task.bind(
            dataset_name=self.dataset_name,
            table_name=self.table_name,
            credentials_key=self.credentials_key,
            start_date=self.start_date,
            end_date=self.end_date,
            date_column_name=self.date_column_name,
            vault_name=self.vault_name,
            credentials_secret=self.credentials_secret,
            flow=self,
        )

        df_with_metadata = add_ingestion_metadata_task.bind(df, flow=self)
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
            overwrite=self.overwrite,
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
            overwrite=self.overwrite,
            sp_credentials_secret=self.adls_sp_credentials_secret,
            flow=self,
        )

        df_with_metadata.set_upstream(df, flow=self)
        dtypes_dict.set_upstream(df_with_metadata, flow=self)
        df_to_be_loaded.set_upstream(dtypes_dict, flow=self)
        file_to_adls_task.set_upstream(df_to_file, flow=self)
        json_to_adls_task.set_upstream(dtypes_to_json_task, flow=self)
        set_key_value(key=self.adls_dir_path, value=self.adls_file_path)
