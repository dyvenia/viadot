import os
from pathlib import Path
from typing import Any, Dict, List

import pendulum
from prefect import Flow, task
from prefect.backend import set_key_value
from prefect.utilities import logging

logger = logging.get_logger()

from viadot.task_utils import (
    add_ingestion_metadata_task,
    df_get_data_types_task,
    df_map_mixed_dtypes_for_parquet,
    df_to_csv,
    df_to_parquet,
    dtypes_to_json_task,
)
from viadot.tasks import AzureDataLakeUpload
from viadot.tasks.sharepoint import SharepointToDF


class SharepointToADLS(Flow):
    def __init__(
        self,
        name: str,
        url_to_file: str = None,
        nrows_to_df: int = None,
        path_to_file: str = None,
        sheet_number: int = None,
        validate_excel_file: bool = False,
        output_file_extension: str = ".csv",
        local_dir_path: str = None,
        adls_dir_path: str = None,
        adls_file_name: str = None,
        adls_sp_credentials_secret: str = None,
        overwrite_adls: bool = False,
        if_empty: str = "warn",
        if_exists: str = "replace",
        timeout: int = 3600,
        *args: List[any],
        **kwargs: Dict[str, Any],
    ):
        """
        Flow for downloading Excel file from Sharepoint then uploading it to Azure Data Lake.

        Args:
            name (str): The name of the flow.
            url_to_file (str, optional): Link to a file on Sharepoint. Defaults to None.
                        (e.g : https://{tenant_name}.sharepoint.com/sites/{folder}/Shared%20Documents/Dashboard/file).
            nrows_to_df (int, optional): Number of rows to read at a time. Defaults to 50000. Defaults to None.
            path_to_file (str, optional): Path to local Excel file. Defaults to None.
            sheet_number (int, optional): Sheet number to be extracted from file. Counting from 0, if None all sheets are axtracted. Defaults to None.
            validate_excel_file (bool, optional): Check if columns in separate sheets are the same. Defaults to False.
            output_file_extension (str, optional): Output file extension - to allow selection of .csv for data which is not easy to handle with parquet. Defaults to ".csv".
            local_dir_path (str, optional): File directory. Defaults to None.
            adls_dir_path (str, optional): Azure Data Lake destination folder/catalog path. Defaults to None.
            adls_file_name (str, optional): Name of file in ADLS. Defaults to None.
            adls_sp_credentials_secret (str, optional): The name of the Azure Key Vault secret containing a dictionary with
            ACCOUNT_NAME and Service Principal credentials (TENANT_ID, CLIENT_ID, CLIENT_SECRET) for the Azure Data Lake.
            Defaults to None.
            overwrite_adls (bool, optional): Whether to overwrite files in the lake. Defaults to False.
            if_empty (str, optional): What to do if query returns no data. Defaults to "warn".
            timeout(int, optional): The amount of time (in seconds) to wait while running this task before
                a timeout occurs. Defaults to 3600.
        """
        # SharepointToDF
        self.if_empty = if_empty
        self.nrows = nrows_to_df
        self.path_to_file = path_to_file
        self.url_to_file = url_to_file
        self.local_dir_path = local_dir_path
        self.sheet_number = sheet_number
        self.validate_excel_file = validate_excel_file
        self.timeout = timeout

        # AzureDataLakeUpload
        self.overwrite = overwrite_adls
        self.adls_sp_credentials_secret = adls_sp_credentials_secret
        self.if_exists = if_exists
        self.output_file_extension = output_file_extension
        self.now = str(pendulum.now("utc"))
        if self.local_dir_path is not None:
            self.local_file_path = (
                self.local_dir_path + self.slugify(name) + self.output_file_extension
            )
        else:
            self.local_file_path = self.slugify(name) + self.output_file_extension
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

    def gen_flow(self) -> Flow:
        excel_to_df_task = SharepointToDF(timeout=self.timeout)
        df = excel_to_df_task.bind(
            path_to_file=self.path_to_file,
            url_to_file=self.url_to_file,
            nrows=self.nrows,
            sheet_number=self.sheet_number,
            validate_excel_file=self.validate_excel_file,
            flow=self,
        )

        df_with_metadata = add_ingestion_metadata_task.bind(df, flow=self)
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

        dtypes_to_json_task.bind(
            dtypes_dict=dtypes_dict, local_json_path=self.local_json_path, flow=self
        )
        json_to_adls_task = AzureDataLakeUpload(timeout=self.timeout)
        json_to_adls_task.bind(
            from_path=self.local_json_path,
            to_path=self.adls_schema_file_dir_file,
            overwrite=self.overwrite,
            sp_credentials_secret=self.adls_sp_credentials_secret,
            flow=self,
        )

        df_mapped.set_upstream(df_with_metadata, flow=self)
        dtypes_to_json_task.set_upstream(df_mapped, flow=self)
        df_to_file.set_upstream(dtypes_to_json_task, flow=self)

        file_to_adls_task.set_upstream(df_to_file, flow=self)
        json_to_adls_task.set_upstream(dtypes_to_json_task, flow=self)
        set_key_value(key=self.adls_dir_path, value=self.adls_file_path)

    @staticmethod
    def slugify(name):
        return name.replace(" ", "_").lower()
