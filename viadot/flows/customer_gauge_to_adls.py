import os
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Literal

import pendulum
from prefect import Flow
from prefect.backend import set_key_value
from prefect.utilities import logging

from viadot.task_utils import (
    add_ingestion_metadata_task,
    anonymize_df,
    df_get_data_types_task,
    df_map_mixed_dtypes_for_parquet,
    df_to_csv,
    df_to_parquet,
    dtypes_to_json_task,
    update_dtypes_dict,
)
from viadot.tasks import AzureDataLakeUpload, CustomerGaugeToDF

logger = logging.get_logger(__name__)


class CustomerGaugeToADLS(Flow):
    def __init__(
        self,
        name: str,
        endpoint: Literal["responses", "non-responses"] = None,
        endpoint_url: str = None,
        total_load: bool = True,
        cursor: int = None,
        pagesize: int = 1000,
        date_field: Literal[
            "date_creation", "date_order", "date_sent", "date_survey_response"
        ] = None,
        start_date: datetime = None,
        end_date: datetime = None,
        customer_gauge_credentials_secret: str = "CUSTOMER-GAUGE",
        anonymize: bool = False,
        columns_to_anonymize: List[str] = None,
        anonymize_method: Literal["mask", "hash"] = "mask",
        anonymize_value: str = "***",
        date_column: str = None,
        days: int = None,
        output_file_extension: str = ".parquet",
        adls_dir_path: str = None,
        local_file_path: str = None,
        adls_file_name: str = None,
        vault_name: str = None,
        adls_sp_credentials_secret: str = None,
        overwrite_adls: bool = False,
        if_exists: str = "replace",
        timeout: int = 3600,
        *args: List[Any],
        **kwargs: Dict[str, Any]
    ):
        """
        Flow for downloading data from the Customer Gauge's endpoints (Responses and Non-Responses) via API to a CSV or Parquet file.
        The data anonimization is optional.Then upload it to Azure Data Lake.

        Args:
            name (str): The name of the flow.
            endpoint (Literal["responses", "non-responses"], optional): Indicate which endpoint to connect. Defaults to None.
            endpoint_url (str, optional): Full URL for pointing to specific endpoint. Defaults to None.
            total_load (bool, optional): Indicate whether to download the data to the latest. If 'False', only one API call is executed (up to 1000 records).
                Defaults to True.
            cursor (int, optional): Cursor value to navigate to the page. Defaults to None.
            pagesize (int, optional): Number of responses (records) returned per page, max value = 1000. Defaults to 1000.
            date_field (Literal["date_creation", "date_order", "date_sent", "date_survey_response"], optional): Specifies the date type which filter date range.
                Defaults to None.
            start_date (datetime, optional): Defines the period start date in yyyy-mm-dd format. Defaults to None.
            end_date (datetime, optional): Defines the period end date in yyyy-mm-dd format. Defaults to None.
            customer_gauge_credentials_secret (str, optional): The name of the Azure Key Vault secret containing a dictionary with ['client_id', 'client_secret'].
                Defaults to "CUSTOMER-GAUGE".
            vault_name (str, optional): The name of the vault from which to obtain the secret. Defaults to None.
            anonymize (bool, optional): Indicates if anonymize selected columns. Defaults to False.
            columns_to_anonymize (List[str], optional): List of columns to anonymize. Defaults to None.
            anonymize_method  (Literal["mask", "hash"], optional): Method of anonymizing data. "mask" -> replace the data with "value" arg.
                "hash" -> replace the data with the hash value of an object (using `hash()` method). Defaults to "mask".
            anonymize_value (str, optional): Value to replace the data. Defaults to "***".
            date_column (str, optional): Name of the date column used to identify rows that are older than a specified number of days. Defaults to None.
            days (int, optional): The number of days beyond which we want to anonymize the data, e.g. older that 2 years can be: 2*365. Defaults to None.
            output_file_extension (str, optional): Output file extension - to allow selection of .csv for data
                which is not easy to handle with parquet. Defaults to ".parquet".
            adls_dir_path (str, optional): Azure Data Lake destination folder/catalog path. Defaults to None.
            local_file_path (str, optional): Local destination path. Defaults to None.
            adls_file_name (str, optional): Name of file in ADLS. Defaults to None.
            adls_sp_credentials_secret (str, optional): The name of the Azure Key Vault secret containing a dictionary with
                ACCOUNT_NAME and Service Principal credentials (TENANT_ID, CLIENT_ID, CLIENT_SECRET) for the Azure Data Lake.
                Defaults to None.
            overwrite_adls (bool, optional): Whether to overwrite files in the lake. Defaults to False.
            if_exists (str, optional): What to do if the file exists. Defaults to "replace".
            timeout (int, optional): The time (in seconds) to wait while running this task before a timeout occurs. Defaults to 3600.
        """
        # CustomerGaugeToDF
        self.endpoint = endpoint
        self.endpoint_url = endpoint_url
        self.total_load = total_load
        self.cursor = cursor
        self.pagesize = pagesize
        self.date_field = date_field
        self.start_date = start_date
        self.end_date = end_date
        self.customer_gauge_credentials_secret = customer_gauge_credentials_secret

        # anonymize_df
        self.anonymize = anonymize
        self.columns_to_anonymize = columns_to_anonymize
        self.anonymize_method = anonymize_method
        self.anonymize_value = anonymize_value
        self.date_column = date_column
        self.days = days

        # AzureDataLakeUpload
        self.adls_file_name = adls_file_name
        self.adls_dir_path = adls_dir_path
        self.local_file_path = local_file_path
        self.overwrite = overwrite_adls
        self.vault_name = vault_name
        self.adls_sp_credentials_secret = adls_sp_credentials_secret
        self.if_exists = if_exists
        self.output_file_extension = output_file_extension
        self.timeout = timeout
        self.now = str(pendulum.now("utc"))

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
        customer_gauge_df_task = CustomerGaugeToDF(
            timeout=self.timeout, endpoint=self.endpoint, endpoint_url=self.endpoint_url
        )

        customerg_df = customer_gauge_df_task.bind(
            total_load=self.total_load,
            cursor=self.cursor,
            pagesize=self.pagesize,
            date_field=self.date_field,
            start_date=self.start_date,
            end_date=self.end_date,
            vault_name=self.vault_name,
            credentials_secret=self.customer_gauge_credentials_secret,
            flow=self,
        )

        if self.anonymize == True:
            anonymized_df = anonymize_df.bind(
                customerg_df,
                columns=self.columns_to_anonymize,
                method=self.anonymize_method,
                value=self.anonymize_value,
                date_column=self.date_column,
                days=self.days,
                flow=self,
            )

            df_with_metadata = add_ingestion_metadata_task.bind(
                anonymized_df, flow=self
            )
        else:
            df_with_metadata = add_ingestion_metadata_task.bind(customerg_df, flow=self)

        dtypes_dict = df_get_data_types_task.bind(df_with_metadata, flow=self)
        df_mapped = df_map_mixed_dtypes_for_parquet.bind(
            df_with_metadata, dtypes_dict, flow=self
        )

        if self.output_file_extension == ".parquet":
            df_to_file = df_to_parquet.bind(
                df=df_mapped,
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

        file_to_adls_task.set_upstream(df_to_file, flow=self)
        json_to_adls_task.set_upstream(dtypes_to_json_task, flow=self)
        set_key_value(key=self.adls_dir_path, value=self.adls_file_path)
