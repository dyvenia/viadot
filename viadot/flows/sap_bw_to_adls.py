import os
from pathlib import Path
from typing import Any, Dict, List, Literal

import pendulum
from prefect import Flow
from prefect.backend import set_key_value

from viadot.task_utils import (
    add_ingestion_metadata_task,
    df_get_data_types_task,
    df_map_mixed_dtypes_for_parquet,
    df_to_csv,
    df_to_parquet,
    dtypes_to_json_task,
    update_dtypes_dict,
)
from viadot.tasks import AzureDataLakeUpload, SAPBWToDF


class SAPBWToADLS(Flow):
    def __init__(
        self,
        name: str,
        mdx_query: str,
        mapping_dict: dict = None,
        sapbw_credentials: dict = None,
        sapbw_credentials_key: str = "SAP",
        env: str = "BW",
        output_file_extension: str = ".parquet",
        local_file_path: str = None,
        adls_file_name: str = None,
        adls_dir_path: str = None,
        if_exists: Literal["replace", "append", "delete"] = "replace",
        overwrite_adls: bool = True,
        vault_name: str = None,
        sp_credentials_secret: str = None,
        *args: List[any],
        **kwargs: Dict[str, Any],
    ):
        """
        Flow for downloading data from SAP BW to file, then uploading it to ADLS.

        Args:
            name (str): Name of the flow.
            mdx_query (str): MDX query to be passed to SAP BW server.
            mapping_dict (dict, optional): Dictionary with original column names and the mapping for them. If not None then flows is generating mapping automatically with mapping applied by user, if not - it generates automatically the json file with columns.
            sapbw_credentials (dict, optional): Credentials to SAP in dictionary format. Defaults to None.
            sapbw_credentials_key (str, optional): Azure KV secret. Defaults to "SAP".
            env (str, optional): SAP environment. Defaults to "BW".
            output_file_extension (str, optional): Output file extension - to allow selection between .csv and .parquet. Defaults to ".parquet".
            local_file_path (str, optional): Local destination path. Defaults to None.
            adls_file_name (str, optional): Azure Data Lake file name. Defaults to None.
            adls_dir_path(str, optional): Azure Data Lake destination file path. Defaults to None.
            if_exists (Literal["append", "replace", "skip"], optional): What to do if the table exists. Defaults to "replace".
            overwrite_adls (bool, optional): Whether to overwrite the file in ADLS. Defaults to True.
            vault_name (str, optional): The name of the vault from which to obtain the secrets.. Defaults to None.
            sp_credentials_secret (str, optional): The name of the Azure Key Vault secret containing a dictionary with ACCOUNT_NAME and Service Principal credentials (TENANT_ID, CLIENT_ID, CLIENT_SECRET). Defaults to None.
        """
        self.sapbw_credentials = sapbw_credentials
        self.sapbw_credentials_key = sapbw_credentials_key
        self.env = env
        self.mdx_query = mdx_query
        self.mapping_dict = mapping_dict
        self.output_file_extension = output_file_extension

        self.local_file_path = (
            local_file_path or self.slugify(name) + self.output_file_extension
        )
        self.local_json_path = self.slugify(name) + ".json"
        self.now = str(pendulum.now("utc"))
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

        self.if_exists = if_exists
        self.overwrite_adls = overwrite_adls
        self.vault_name = vault_name
        self.sp_credentials_secret = sp_credentials_secret

        super().__init__(*args, name=name, **kwargs)
        self.gen_flow()

    @staticmethod
    def slugify(name):
        return name.replace(" ", "_").lower()

    def gen_flow(self) -> Flow:
        sapbw_to_df_task = SAPBWToDF(
            sapbw_credentials=self.sapbw_credentials,
            sapbw_credentials_key=self.sapbw_credentials_key,
            env=self.env,
        )

        df = sapbw_to_df_task.bind(
            mdx_query=self.mdx_query,
            mapping_dict=self.mapping_dict,
            flow=self,
        )

        df_viadot_downloaded = add_ingestion_metadata_task.bind(df=df, flow=self)
        dtypes_dict = df_get_data_types_task.bind(df_viadot_downloaded, flow=self)

        df_to_be_loaded = df_map_mixed_dtypes_for_parquet.bind(
            df_viadot_downloaded, dtypes_dict, flow=self
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
                df=df_to_be_loaded,
                path=self.local_file_path,
                if_exists=self.if_exists,
                flow=self,
            )

        file_to_adls_task = AzureDataLakeUpload()
        adls_upload = file_to_adls_task.bind(
            from_path=self.local_file_path,
            to_path=self.adls_file_path,
            overwrite=self.overwrite_adls,
            sp_credentials_secret=self.sp_credentials_secret,
            flow=self,
        )

        dtypes_updated = update_dtypes_dict(dtypes_dict, flow=self)
        dtypes_to_json_task.bind(
            dtypes_dict=dtypes_updated, local_json_path=self.local_json_path, flow=self
        )

        json_to_adls_task = AzureDataLakeUpload()
        json_to_adls_task.bind(
            from_path=self.local_json_path,
            to_path=self.adls_schema_file_dir_file,
            overwrite=self.overwrite_adls,
            sp_credentials_secret=self.sp_credentials_secret,
            vault_name=self.vault_name,
            flow=self,
        )

        df_viadot_downloaded.set_upstream(df, flow=self)
        dtypes_dict.set_upstream(df_viadot_downloaded, flow=self)
        df_to_be_loaded.set_upstream(dtypes_dict, flow=self)
        adls_upload.set_upstream(df_to_file, flow=self)

        df_to_file.set_upstream(dtypes_updated, flow=self)
        json_to_adls_task.set_upstream(dtypes_to_json_task, flow=self)

        set_key_value(key=self.adls_dir_path, value=self.adls_file_path)
