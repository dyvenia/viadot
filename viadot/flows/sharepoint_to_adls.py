import os
from pathlib import Path
from typing import Any, Dict, List, Literal

import pendulum
from prefect import Flow, task, case
from prefect.engine.state import Failed
from prefect.engine.runner import ENDRUN
from typing import Literal
from prefect.backend import set_key_value
from prefect.utilities import logging

from viadot.task_utils import (
    add_ingestion_metadata_task,
    df_get_data_types_task,
    df_map_mixed_dtypes_for_parquet,
    df_to_csv,
    df_to_parquet,
    dtypes_to_json_task,
    validate_df,
)
from viadot.tasks import AzureDataLakeUpload
from viadot.tasks.sharepoint import SharepointListToDF, SharepointToDF
from viadot.task_utils import check_if_df_empty

logger = logging.get_logger()


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
        validate_df_dict: dict = None,
        timeout: int = 3600,
        set_prefect_kv: bool = False,
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
            validate_df_dict (dict, optional): A dictionary with optional list of tests to verify the output
            dataframe. If defined, triggers the `validate_df` task from task_utils. Defaults to None.
            timeout(int, optional): The amount of time (in seconds) to wait while running this task before
                a timeout occurs. Defaults to 3600.
            set_prefect_kv (bool, optional): Whether to do key-value parameters in KV Store or not. Defaults to False.
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
        self.validate_df_dict = validate_df_dict

        # AzureDataLakeUpload
        self.overwrite = overwrite_adls
        self.adls_sp_credentials_secret = adls_sp_credentials_secret
        self.if_exists = if_exists
        self.output_file_extension = output_file_extension
        self.set_prefect_kv = set_prefect_kv
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

        if self.validate_df_dict:
            validation_task = validate_df(df=df, tests=self.validate_df_dict, flow=self)
            validation_task.set_upstream(df, flow=self)

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

        if self.validate_df_dict:
            df_with_metadata.set_upstream(validation_task, flow=self)

        df_mapped.set_upstream(df_with_metadata, flow=self)
        dtypes_to_json_task.set_upstream(df_mapped, flow=self)
        df_to_file.set_upstream(dtypes_to_json_task, flow=self)

        file_to_adls_task.set_upstream(df_to_file, flow=self)
        json_to_adls_task.set_upstream(dtypes_to_json_task, flow=self)
        if self.set_prefect_kv == True:
            set_key_value(key=self.adls_dir_path, value=self.adls_file_path)

    @staticmethod
    def slugify(name):
        return name.replace(" ", "_").lower()


class SharepointListToADLS(Flow):
    def __init__(
        self,
        name: str,
        list_title: str,
        site_url: str,
        file_name: str,
        adls_dir_path: str,
        filters: dict = None,
        required_fields: List[str] = None,
        field_property: str = "Title",
        row_count: int = 5000,
        adls_sp_credentials_secret: str = None,
        sp_cert_credentials_secret: str = None,
        vault_name: str = None,
        overwrite_adls: bool = True,
        output_file_extension: Literal[".parquet", ".csv"] = ".parquet",
        sep: str = "\t",
        validate_df_dict: dict = None,
        set_prefect_kv: bool = False,
        if_no_data_returned: Literal["skip", "warn", "fail"] = "skip",
        *args: List[any],
        **kwargs: Dict[str, Any],
    ):
        """
        Flow for ingesting sharepoint list items(rows) with a given (or all) columns.
        It allows to filter the output by column values.
        Data is ingested from MS Sharepoint list (with given name and url ) and stored in MS Azure ADLS.

        Args:
            name (str): Prefect flow name.
            list_title (str): Title of Sharepoint List.
            site_url (str): URL to set of Sharepoint Lists.
            file_name (str): Name of file in ADLS. Defaults to None.
            adls_dir_path (str): Azure Data Lake destination folder/catalog path. Defaults to None.
            filters (dict, optional): Dictionary with operators which filters the SharepointList output. Defaults to None.
                        allowed dtypes: ('datetime','date','bool','int', 'float', 'complex', 'str')
                        allowed conjunction: ('&','|')
                        allowed operators: ('<','>','<=','>=','==','!=')
                        Example how to build the dict:
                        filters = {
                        'Column_name_1' :
                                {
                                'dtype': 'datetime',
                                'value1':'YYYY-MM-DD',
                                'value2':'YYYY-MM-DD',
                                'operator1':'>=',
                                'operator2':'<=',
                                'operators_conjunction':'&', # conjunction operators allowed only when 2 values passed
                                'filters_conjunction':'&', # conjunction filters allowed only when 2 columns passed
                                }
                                ,
                        'Column_name_2' :
                                {
                                'dtype': 'str',
                                'value1':'NM-PL',
                                'operator1':'==',
                                },
                        }
            required_fields (List[str], optional): Required fields(columns) need to be extracted from
                                     Sharepoint List. Defaults to None.
            field_property (str, optional): Property to expand fields with expand query method.
                                    For example: User fields could be expanded and "Title"
                                    or "ID" could be extracted
                                    -> useful to get user name instead of ID
                                    All properties can be found under list.item.properties.
                                    WARNING! Field types and properties might change which could
                                    lead to errors - extension of sp connector would be required.
                                    Default to ["Title"]. Defaults to "Title".
            row_count (int, optional): Number of downloaded rows in single request.Defaults to 5000.
            adls_sp_credentials_secret (str, optional): Credentials to connect to Azure ADLS
                                    If not passed it will take cred's from your .config/credentials.json Defaults to None.
            sp_cert_credentials_secret (str, optional): Credentials to verify Sharepoint connection.
                                    If not passed it will take cred's from your .config/credentials.json Default to None.
            vault_name (str, optional): KeyVaultSecret name. Default to None.
            overwrite_adls (bool, optional): Whether to overwrite files in the lake. Defaults to True.
            output_file_extension (str, optional): Extension of the resulting file to be stored, either ".csv" or ".parquet". Defaults to ".parquet".
            sep (str, optional): The separator to use in the CSV. Defaults to "\t".
            validate_df_dict (dict, optional): Whether to do an extra df validation before ADLS upload or not to do. Defaults to None.
            set_prefect_kv (bool, optional): Whether to do key-value parameters in KV Store or not. Defaults to False.

        Returns:
            .parquet file inside ADLS.
        """

        # SharepointListToDF
        self.file_name = file_name
        self.list_title = list_title
        self.site_url = site_url
        self.required_fields = required_fields
        self.field_property = field_property
        self.filters = filters
        self.sp_cert_credentials_secret = sp_cert_credentials_secret
        self.vault_name = vault_name
        self.row_count = row_count
        self.validate_df_dict = validate_df_dict
        self.if_no_data_returned = if_no_data_returned

        # AzureDataLakeUpload
        self.adls_dir_path = adls_dir_path
        self.overwrite = overwrite_adls
        self.adls_sp_credentials_secret = adls_sp_credentials_secret
        self.output_file_extension = output_file_extension
        self.sep = sep
        self.set_prefect_kv = set_prefect_kv
        self.now = str(pendulum.now("utc"))
        if self.file_name is not None:
            self.local_file_path = (
                self.file_name.split(".")[0] + self.output_file_extension
            )
            self.adls_file_path = os.path.join(adls_dir_path, file_name)
            self.adls_schema_file_dir_file = os.path.join(
                adls_dir_path, "schema", Path(file_name).stem + ".json"
            )
        else:
            self.local_file_path = self.slugify(name) + self.output_file_extension
            self.adls_file_path = os.path.join(
                adls_dir_path, self.now + self.output_file_extension
            )
            self.adls_schema_file_dir_file = os.path.join(
                adls_dir_path, "schema", self.now + ".json"
            )
        self.local_json_path = self.slugify(name) + ".json"
        self.adls_dir_path = adls_dir_path

        super().__init__(
            name=name,
            *args,
            **kwargs,
        )

        self.gen_flow()

    def gen_flow(self) -> Flow:
        df = SharepointListToDF(
            path=self.file_name,
            list_title=self.list_title,
            site_url=self.site_url,
            required_fields=self.required_fields,
            field_property=self.field_property,
            filters=self.filters,
            row_count=self.row_count,
            credentials_secret=self.sp_cert_credentials_secret,
        )

        df_empty = check_if_df_empty.bind(df, self.if_no_data_returned, flow=self)

        with case(df_empty, False):
            if self.validate_df_dict:
                validation_task = validate_df(
                    df=df, tests=self.validate_df_dict, flow=self
                )
                validation_task.set_upstream(df, flow=self)

            df_with_metadata = add_ingestion_metadata_task.bind(df, flow=self)
            dtypes_dict = df_get_data_types_task.bind(df_with_metadata, flow=self)
            df_mapped = df_map_mixed_dtypes_for_parquet.bind(
                df_with_metadata, dtypes_dict, flow=self
            )

            if self.output_file_extension == ".csv":
                df_to_file = df_to_csv.bind(
                    df=df_with_metadata,
                    path=self.local_file_path,
                    sep=self.sep,
                    flow=self,
                )
            elif self.output_file_extension == ".parquet":
                df_to_file = df_to_parquet.bind(
                    df=df_mapped,
                    path=self.local_file_path,
                    flow=self,
                )
            else:
                raise ValueError(
                    "Output file extension can only be '.csv' or '.parquet'"
                )

            file_to_adls_task = AzureDataLakeUpload()
            file_to_adls_task.bind(
                from_path=self.path,
                to_path=self.adls_dir_path,
                overwrite=self.overwrite,
                sp_credentials_secret=self.adls_sp_credentials_secret,
                flow=self,
            )

            dtypes_to_json_task.bind(
                dtypes_dict=dtypes_dict, local_json_path=self.local_json_path, flow=self
            )

            json_to_adls_task = AzureDataLakeUpload()
            json_to_adls_task.bind(
                from_path=self.local_json_path,
                to_path=self.adls_schema_file_dir_file,
                overwrite=self.overwrite,
                sp_credentials_secret=self.adls_sp_credentials_secret,
                flow=self,
            )

            if self.validate_df_dict:
                df_with_metadata.set_upstream(validation_task, flow=self)
            dtypes_dict.set_upstream(df_with_metadata, flow=self)
            df_mapped.set_upstream(df_with_metadata, flow=self)
            dtypes_to_json_task.set_upstream(df_mapped, flow=self)
            df_to_file.set_upstream(dtypes_to_json_task, flow=self)
            file_to_adls_task.set_upstream(df_to_file, flow=self)
            json_to_adls_task.set_upstream(dtypes_to_json_task, flow=self)
            if self.set_prefect_kv == True:
                set_key_value(key=self.adls_dir_path, value=self.adls_file_path)

    @staticmethod
    def slugify(name):
        return name.replace(" ", "_").lower()
