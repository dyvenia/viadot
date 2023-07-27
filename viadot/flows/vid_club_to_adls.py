import os
from pathlib import Path
from typing import Any, Dict, List, Literal

import pandas as pd
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
from viadot.tasks import AzureDataLakeUpload, VidClubToDF

logger = logging.get_logger(__name__)


class VidClubToADLS(Flow):
    def __init__(
        self,
        name: str,
        source: Literal["jobs", "product", "company", "survey"] = None,
        from_date: str = "2022-03-22",
        to_date: str = None,
        items_per_page: int = 100,
        region: Literal["bg", "hu", "hr", "pl", "ro", "si", "all"] = "all",
        days_interval: int = 30,
        cols_to_drop: List[str] = None,
        vid_club_credentials: Dict[str, Any] = None,
        vidclub_credentials_secret: str = "VIDCLUB",
        vidclub_vault_name: str = None,
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
        Flow for downloading data from the Vid Club via API to a CSV or Parquet file.
        Then upload it to Azure Data Lake.

        Args:
            name (str): The name of the flow.
            source (Literal["jobs", "product", "company", "survey"], optional): The endpoint source to be accessed. Defaults to None.
            from_date (str, optional): Start date for the query, by default is the oldest date in the data 2022-03-22.
            to_date (str, optional): End date for the query. By default None, which will be executed as datetime.today().strftime("%Y-%m-%d") in code.
            items_per_page (int, optional): Number of entries per page. Defaults to 100.
            region (Literal["bg", "hu", "hr", "pl", "ro", "si", "all"], optional): Region filter for the query. Defaults to "all". [July 2023 status: parameter works only for 'all' on API]
            days_interval (int, optional): Days specified in date range per API call (test showed that 30-40 is optimal for performance). Defaults to 30.
            cols_to_drop (List[str], optional): List of columns to drop. Defaults to None.
            vid_club_credentials (Dict[str, Any], optional): Stores the credentials information. Defaults to None.
            vidclub_credentials_secret (str, optional): The name of the secret in Azure Key Vault or Prefect or local_config file. Defaults to "VIDCLUB".
            vidclub_vault_name (str, optional): For Vid Club credentials stored in Azure Key Vault. The name of the vault from which to obtain the secret. Defaults to None.
            output_file_extension (str, optional): Output file extension - to allow selection of .csv for data
                which is not easy to handle with parquet. Defaults to ".parquet".
            adls_dir_path (str, optional): Azure Data Lake destination folder/catalog path. Defaults to None.
            local_file_path (str, optional): Local destination path. Defaults to None.
            adls_file_name (str, optional): Name of file in ADLS. Defaults to None.
            vault_name (str, optional): For ADLS credentials stored in Azure Key Vault. The name of the vault from which to obtain the secret. Defaults to None.
            adls_sp_credentials_secret (str, optional): The name of the Azure Key Vault secret containing a dictionary with
                ACCOUNT_NAME and Service Principal credentials (TENANT_ID, CLIENT_ID, CLIENT_SECRET) for the Azure Data Lake.
                Defaults to None.
            overwrite_adls (bool, optional): Whether to overwrite files in the lake. Defaults to False.
            if_exists (str, optional): What to do if the file exists. Defaults to "replace".
            timeout (int, optional): The time (in seconds) to wait while running this task before a timeout occurs. Defaults to 3600.
        """
        # VidClubToDF
        self.source = source
        self.from_date = from_date
        self.to_date = to_date
        self.items_per_page = items_per_page
        self.region = region
        self.days_interval = days_interval
        self.cols_to_drop = cols_to_drop
        self.vid_club_credentials = vid_club_credentials
        self.vidclub_credentials_secret = vidclub_credentials_secret
        self.vidclub_vault_name = vidclub_vault_name

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
        vid_club_df_task = VidClubToDF(
            timeout=self.timeout,
            source=self.source,
            credentials=self.vid_club_credentials,
            credentials_secret=self.vidclub_credentials_secret,
            vault_name=self.vidclub_vault_name,
        )

        vid_club_df = vid_club_df_task.bind(
            from_date=self.from_date,
            to_date=self.to_date,
            items_per_page=self.items_per_page,
            region=self.region,
            days_interval=self.days_interval,
            cols_to_drop=self.cols_to_drop,
            flow=self,
        )

        df_with_metadata = add_ingestion_metadata_task.bind(vid_club_df, flow=self)

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
