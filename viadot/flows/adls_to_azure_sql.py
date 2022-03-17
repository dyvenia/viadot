import json
import os
from typing import Any, Dict, List, Literal

import pandas as pd
from prefect import Flow, task
from prefect.backend import get_key_value
from prefect.utilities import logging

from viadot.tasks.azure_data_lake import AzureDataLakeDownload

from ..tasks import (
    AzureDataLakeCopy,
    AzureDataLakeToDF,
    AzureSQLCreateTable,
    BCPTask,
    DownloadGitHubFile,
    AzureSQLDBQuery,
    CheckColumnOrder,
)

logger = logging.get_logger(__name__)

lake_to_df_task = AzureDataLakeToDF()
download_json_file_task = AzureDataLakeDownload()
download_github_file_task = DownloadGitHubFile()
promote_to_conformed_task = AzureDataLakeCopy()
promote_to_operations_task = AzureDataLakeCopy()
create_table_task = AzureSQLCreateTable()
bulk_insert_task = BCPTask()
azure_query_task = AzureSQLDBQuery()
check_column_order_task = CheckColumnOrder()


@task
def union_dfs_task(dfs: List[pd.DataFrame]):
    return pd.concat(dfs, ignore_index=True)


@task
def map_data_types_task(json_shema_path: str):
    file_dtypes = open(json_shema_path)
    dict_dtypes = json.load(file_dtypes)
    dict_mapping = {
        "Float": "REAL",
        "Image": None,
        "Categorical": "VARCHAR(500)",
        "Time": "TIME",
        "Boolean": "BIT",
        "DateTime": "DATETIMEOFFSET",  # DATETIMEOFFSET is the only timezone-aware dtype in TSQL
        "Object": "VARCHAR(500)",
        "EmailAddress": "VARCHAR(50)",
        "File": None,
        "Geometry": "GEOMETRY",
        "Ordinal": "VARCHAR(500)",
        "Integer": "INT",
        "Generic": "VARCHAR(500)",
        "UUID": "UNIQUEIDENTIFIER",
        "Complex": None,
        "Date": "DATE",
        "String": "VARCHAR(500)",
        "IPAddress": "VARCHAR(39)",
        "Path": "VARCHAR(500)",
        "TimeDelta": "VARCHAR(20)",  # datetime.datetime.timedelta; eg. '1 days 11:00:00'
        "URL": "VARCHAR(500)",
        "Count": "INT",
    }
    dict_dtypes_mapped = {}
    for k in dict_dtypes:
        dict_dtypes_mapped[k] = dict_mapping[dict_dtypes[k]]
    return dict_dtypes_mapped


@task
def df_to_csv_task(df, remove_tab, path: str, sep: str = "\t"):
    # if table doesn't exist it will be created later -  df equals None
    if df is None:
        logger.warning("DataFrame is None")
    else:
        if remove_tab == True:
            for col in range(len(df.columns)):
                df[df.columns[col]] = (
                    df[df.columns[col]].astype(str).str.replace(r"\t", "", regex=True)
                )
            df.to_csv(path, sep=sep, index=False)
        else:
            df.to_csv(path, sep=sep, index=False)


class ADLSToAzureSQL(Flow):
    def __init__(
        self,
        name: str,
        local_file_path: str = None,
        adls_path: str = None,
        read_sep: str = "\t",
        write_sep: str = "\t",
        remove_tab: bool = False,
        overwrite_adls: bool = True,
        if_empty: str = "warn",
        adls_sp_credentials_secret: str = None,
        dtypes: Dict[str, Any] = None,
        table: str = None,
        schema: str = None,
        if_exists: Literal["fail", "replace", "append", "delete"] = "replace",
        check_col_order: bool = True,
        sqldb_credentials_secret: str = None,
        max_download_retries: int = 5,
        tags: List[str] = ["promotion"],
        vault_name: str = None,
        *args: List[any],
        **kwargs: Dict[str, Any],
    ):
        """
        Flow for downloading data from different marketing APIs to a local CSV
        using Supermetrics API, then uploading it to Azure Data Lake,
        and finally inserting into Azure SQL Database.

        Args:
            name (str): The name of the flow.
            local_file_path (str, optional): Local destination path. Defaults to None.
            adls_path (str): The path to an ADLS folder or file. If you pass a path to a directory,
            the latest file from that directory will be loaded. We assume that the files are named using timestamps.
            read_sep (str, optional): The delimiter for the source file. Defaults to "\t".
            write_sep (str, optional): The delimiter for the output CSV file. Defaults to "\t".
            remove_tab (bool, optional): Whether to remove tab delimiters from the data. Defaults to False.
            overwrite_adls (bool, optional): Whether to overwrite the file in ADLS. Defaults to True.
            if_empty (str, optional): What to do if the Supermetrics query returns no data. Defaults to "warn".
            adls_sp_credentials_secret (str, optional): The name of the Azure Key Vault secret containing a dictionary with
            ACCOUNT_NAME and Service Principal credentials (TENANT_ID, CLIENT_ID, CLIENT_SECRET) for the Azure Data Lake.
            Defaults to None.
            dtypes (dict, optional): Which custom data types should be used for SQL table creation task.
            To be used only in case that dtypes need to be manually mapped - dtypes from raw schema file in use by default. Defaults to None.
            table (str, optional): Destination table. Defaults to None.
            schema (str, optional): Destination schema. Defaults to None.
            if_exists (Literal, optional): What to do if the table exists. Defaults to "replace".
            check_col_order (bool, optional): Whether to check column order. Defaults to True.
            sqldb_credentials_secret (str, optional): The name of the Azure Key Vault secret containing a dictionary with
            Azure SQL Database credentials. Defaults to None.
            max_download_retries (int, optional): How many times to retry the download. Defaults to 5.
            tags (List[str], optional): Flow tags to use, eg. to control flow concurrency. Defaults to ["promotion"].
            vault_name (str, optional): The name of the vault from which to obtain the secrets. Defaults to None.
        """
        adls_path = adls_path.strip("/")
        # Read parquet
        if adls_path.split(".")[-1] in ["csv", "parquet"]:
            self.adls_path = adls_path
        else:
            self.adls_path = get_key_value(key=adls_path)

        # Read schema
        self.dtypes = dtypes
        self.adls_root_dir_path = os.path.split(self.adls_path)[0]
        self.adls_file_name = os.path.split(self.adls_path)[-1]
        extension = os.path.splitext(self.adls_path)[-1]
        json_schema_file_name = self.adls_file_name.replace(extension, ".json")
        self.json_shema_path = os.path.join(
            self.adls_root_dir_path,
            "schema",
            json_schema_file_name,
        )

        # AzureDataLakeUpload
        self.local_file_path = local_file_path or self.slugify(name) + ".csv"
        self.local_json_path = self.slugify(name) + ".json"
        self.read_sep = read_sep
        self.write_sep = write_sep
        self.overwrite_adls = overwrite_adls
        self.if_empty = if_empty
        self.adls_sp_credentials_secret = adls_sp_credentials_secret
        self.adls_path_conformed = self.get_promoted_path(env="conformed")
        self.adls_path_operations = self.get_promoted_path(env="operations")

        # AzureSQLCreateTable
        self.table = table
        self.schema = schema
        self.if_exists = self._map_if_exists(if_exists)
        self.check_col_order = check_col_order
        # Generate CSV
        self.remove_tab = remove_tab

        # BCPTask
        self.sqldb_credentials_secret = sqldb_credentials_secret

        # Global
        self.max_download_retries = max_download_retries
        self.tags = tags
        self.vault_name = vault_name

        super().__init__(*args, name=name, **kwargs)

        # self.dtypes.update(METADATA_COLUMNS)
        self.gen_flow()

    @staticmethod
    def _map_if_exists(if_exists: str) -> str:
        mapping = {"append": "skip"}
        return mapping.get(if_exists, if_exists)

    @staticmethod
    def slugify(name):
        return name.replace(" ", "_").lower()

    def get_promoted_path(self, env: str) -> str:
        adls_path_clean = self.adls_path.strip("/")
        extension = adls_path_clean.split(".")[-1].strip()
        if extension == "parquet":
            file_name = adls_path_clean.split("/")[-2] + ".csv"
            common_path = "/".join(adls_path_clean.split("/")[1:-2])
        else:
            file_name = adls_path_clean.split("/")[-1]
            common_path = "/".join(adls_path_clean.split("/")[1:-1])

        promoted_path = os.path.join(env, common_path, file_name)

        return promoted_path

    def gen_flow(self) -> Flow:
        df = lake_to_df_task.bind(
            path=self.adls_path,
            sp_credentials_secret=self.adls_sp_credentials_secret,
            sep=self.read_sep,
            flow=self,
        )

        if not self.dtypes:
            download_json_file_task.bind(
                from_path=self.json_shema_path,
                to_path=self.local_json_path,
                sp_credentials_secret=self.adls_sp_credentials_secret,
                flow=self,
            )
            dtypes = map_data_types_task.bind(self.local_json_path, flow=self)
            map_data_types_task.set_upstream(download_json_file_task, flow=self)
        else:
            dtypes = self.dtypes

        df_reorder = check_column_order_task.bind(
            table=self.table,
            schema=self.schema,
            df=df,
            if_exists=self.if_exists,
            credentials_secret=self.sqldb_credentials_secret,
            flow=self,
        )
        if self.check_col_order == False:
            df_to_csv = df_to_csv_task.bind(
                df=df,
                path=self.local_file_path,
                sep=self.write_sep,
                remove_tab=self.remove_tab,
                flow=self,
            )
        else:
            df_to_csv = df_to_csv_task.bind(
                df=df_reorder,
                path=self.local_file_path,
                sep=self.write_sep,
                remove_tab=self.remove_tab,
                flow=self,
            )

        promote_to_conformed_task.bind(
            from_path=self.adls_path,
            to_path=self.adls_path_conformed,
            sp_credentials_secret=self.adls_sp_credentials_secret,
            vault_name=self.vault_name,
            flow=self,
        )
        promote_to_operations_task.bind(
            from_path=self.adls_path_conformed,
            to_path=self.adls_path_operations,
            sp_credentials_secret=self.adls_sp_credentials_secret,
            vault_name=self.vault_name,
            flow=self,
        )
        create_table_task.bind(
            schema=self.schema,
            table=self.table,
            dtypes=dtypes,
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

        df_reorder.set_upstream(lake_to_df_task, flow=self)
        df_to_csv.set_upstream(df_reorder, flow=self)
        promote_to_conformed_task.set_upstream(df_to_csv, flow=self)
        promote_to_conformed_task.set_upstream(df_to_csv, flow=self)
        create_table_task.set_upstream(df_to_csv, flow=self)
        promote_to_operations_task.set_upstream(promote_to_conformed_task, flow=self)
        bulk_insert_task.set_upstream(create_table_task, flow=self)
