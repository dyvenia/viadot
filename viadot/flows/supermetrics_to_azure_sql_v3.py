from typing import Any, Dict, List, Union

import pandas as pd
from prefect import Flow, Task, apply_map, task
from prefect.utilities import logging

from ..task_utils import METADATA_COLUMNS, add_ingestion_metadata_task
from ..tasks import (
    AzureDataLakeUpload,
    AzureSQLCreateTable,
    BCPTask,
    DownloadGitHubFile,
    RunGreatExpectationsValidation,
    SupermetricsToDF,
)

logger = logging.get_logger(__name__)

supermetrics_to_df_task = SupermetricsToDF()
download_github_file_task = DownloadGitHubFile()
validation_task = RunGreatExpectationsValidation()
csv_to_adls_task = AzureDataLakeUpload()
create_table_task = AzureSQLCreateTable()
bulk_insert_task = BCPTask()


@task
def union_dfs_task(dfs: List[pd.DataFrame]):
    return pd.concat(dfs, ignore_index=True)


class SupermetricsToAzureSQLv3(Flow):
    def __init__(
        self,
        name: str,
        ds_id: str,
        ds_accounts: List[str],
        ds_user: str,
        fields: List[str],
        ds_segments: List[str] = None,
        date_range_type: str = None,
        settings: Dict[str, Any] = None,
        filter: str = None,
        max_rows: int = 1000000,
        max_columns: int = None,
        order_columns: str = None,
        expectation_suite_path: str = None,
        local_file_path: str = None,
        adls_path: str = None,
        sep: str = "\t",
        overwrite_adls: bool = True,
        if_empty: str = "warn",
        adls_sp_credentials_secret: str = None,
        table: str = None,
        schema: str = None,
        dtypes: Dict[str, Any] = None,
        if_exists: str = "replace",  # this applies to the full CSV file, not per chunk
        sqldb_credentials_secret: str = None,
        max_download_retries: int = 5,
        supermetrics_task_timeout: int = 60 * 30,
        parallel: bool = True,
        tags: List[str] = ["extract"],
        vault_name: str = None,
        *args: List[any],
        **kwargs: Dict[str, Any]
    ):
        """
        Flow for downloading data from different marketing APIs to a local CSV
        using Supermetrics API, then uploading it to Azure Data Lake,
        and finally inserting into Azure SQL Database.

        Args:
            name (str): The name of the flow.
            ds_id (str): A query parameter passed to the SupermetricsToCSV task
            ds_accounts (List[str]): A query parameter passed to the SupermetricsToCSV task
            ds_user (str): A query parameter passed to the SupermetricsToCSV task
            fields (List[str]): A query parameter passed to the SupermetricsToCSV task
            ds_segments (List[str], optional): A query parameter passed to the SupermetricsToCSV task. Defaults to None.
            date_range_type (str, optional): A query parameter passed to the SupermetricsToCSV task. Defaults to None.
            settings (Dict[str, Any], optional): A query parameter passed to the SupermetricsToCSV task. Defaults to None.
            filter (str, optional): A query parameter passed to the SupermetricsToCSV task. Defaults to None.
            max_rows (int, optional): A query parameter passed to the SupermetricsToCSV task. Defaults to 1000000.
            max_columns (int, optional): A query parameter passed to the SupermetricsToCSV task. Defaults to None.
            order_columns (str, optional): A query parameter passed to the SupermetricsToCSV task. Defaults to None.
            expectation_suite_path (str, optional): The path to the expectations suite JSON file.
            Currently, only GitHub URLs are supported. Defaults to None.
            local_file_path (str, optional): Local destination path. Defaults to None.
            adls_path (str, optional): Azure Data Lake destination path. Defaults to None.
            sep (str, optional): The separator to use in the CSV. Defaults to "\t".
            overwrite_adls (bool, optional): Whether to overwrite the file in ADLS. Defaults to True.
            if_empty (str, optional): What to do if the Supermetrics query returns no data. Defaults to "warn".
            adls_sp_credentials_secret (str, optional): The name of the Azure Key Vault secret containing a dictionary with
            ACCOUNT_NAME and Service Principal credentials (TENANT_ID, CLIENT_ID, CLIENT_SECRET) for the Azure Data Lake.
            Defaults to None.
            table (str, optional): Destination table. Defaults to None.
            schema (str, optional): Destination schema. Defaults to None.
            dtypes (Dict[str, Any], optional): The data types to use for the table. Defaults to None.
            if_exists (str, optional): What to do if the table exists. Defaults to "replace".
            sqldb_credentials_secret (str, optional): The name of the Azure Key Vault secret containing a dictionary with
            Azure SQL Database credentials. Defaults to None.
            max_download_retries (int, optional): How many times to retry the download. Defaults to 5.
            supermetrics_task_timeout (int, optional): The timeout for the download task. Defaults to 60*30.
            parallel (bool, optional): Whether to parallelize the downloads. Defaults to True.
            tags (List[str], optional): Flow tags to use, eg. to control flow concurrency. Defaults to ["extract"].
            vault_name (str, optional): The name of the vault from which to obtain the secrets. Defaults to None.
        """
        # SupermetricsToDF
        self.ds_id = ds_id
        self.ds_accounts = ds_accounts
        self.ds_segments = ds_segments
        self.ds_user = ds_user
        self.fields = fields
        self.date_range_type = date_range_type
        self.settings = settings
        self.filter = filter
        self.max_rows = max_rows
        self.max_columns = max_columns
        self.order_columns = order_columns

        # DownloadGitHubFile (download expectations)
        self.expectation_suite_path = expectation_suite_path

        # AzureDataLakeUpload
        self.local_file_path = local_file_path or self.slugify(name) + ".csv"
        self.adls_path = adls_path
        self.sep = sep
        self.overwrite_adls = overwrite_adls
        self.if_empty = if_empty
        self.adls_sp_credentials_secret = adls_sp_credentials_secret

        # BCPTask
        self.table = table
        self.schema = schema
        self.dtypes = dtypes
        self.if_exists = if_exists
        self.sqldb_credentials_secret = sqldb_credentials_secret

        # Global
        self.max_download_retries = max_download_retries
        self.supermetrics_task_timeout = supermetrics_task_timeout
        self.parallel = parallel
        self.tags = tags
        self.vault_name = vault_name

        super().__init__(*args, name=name, **kwargs)
        self.dtypes.update(METADATA_COLUMNS)
        self.gen_flow()

    @staticmethod
    def slugify(name):
        return name.replace(" ", "_").lower()

    def gen_supermetrics_task(
        self, ds_accounts: Union[str, List[str]], flow: Flow = None
    ) -> Task:

        t = supermetrics_to_df_task.bind(
            ds_id=self.ds_id,
            ds_accounts=ds_accounts,
            ds_segments=self.ds_segments,
            ds_user=self.ds_user,
            fields=self.fields,
            date_range_type=self.date_range_type,
            settings=self.settings,
            filter=self.filter,
            max_rows=self.max_rows,
            max_columns=self.max_columns,
            order_columns=self.order_columns,
            if_empty=self.if_empty,
            max_retries=self.max_download_retries,
            timeout=self.supermetrics_task_timeout,
            flow=flow,
        )
        return t

    def gen_flow(self) -> Flow:
        if self.parallel:
            # generate a separate task for each account
            dfs = apply_map(self.gen_supermetrics_task, self.ds_accounts, flow=self)
        else:
            dfs = self.gen_supermetrics_task(ds_accounts=self.ds_accounts, flow=self)
        df = union_dfs_task.bind(dfs)
        download_expectations = download_github_file_task.bind(
            repo="dyvenia/workstreams_vel",
            from_path="/flows/google_ads_fr_extract/expectations/failure.json",
            to_path="failure.json",
            flow=self,
        )
        validation = validation_task.bind(
            df=df,
            expectations_path=expectations_path,
            expectation_suite_name=expectation_suite_name,
            upstream_tasks=[download_expectations_suite],
        )

        # df_to_csv_task.bind(
        #     df=df,
        #     path=self.local_file_path,
        #     if_exists=if_exists,
        #     sep=self.sep,
        # )

        # add_ingestion_metadata_task.bind(
        #     path=self.local_file_path, sep=self.sep, flow=self
        # )

        # csv_to_adls_task.bind(
        #     from_path=self.local_file_path,
        #     to_path=self.adls_path,
        #     overwrite=self.overwrite_adls,
        #     sp_credentials_secret=self.adls_sp_credentials_secret,
        #     vault_name=self.vault_name,
        #     flow=self,
        # )
        # create_table_task.bind(
        #     schema=self.schema,
        #     table=self.table,
        #     dtypes=self.dtypes,
        #     if_exists=self.if_exists,
        #     credentials_secret=self.sqldb_credentials_secret,
        #     vault_name=self.vault_name,
        #     flow=self,
        # )
        # bulk_insert_task.bind(
        #     path=self.local_file_path,
        #     schema=self.schema,
        #     table=self.table,
        #     credentials_secret=self.sqldb_credentials_secret,
        #     vault_name=self.vault_name,
        #     flow=self,
        # )

        add_ingestion_metadata_task.set_upstream(supermetrics_downloads, flow=self)
        # csv_to_adls_task.set_upstream(add_ingestion_metadata_task, flow=self)
        # create_table_task.set_upstream(add_ingestion_metadata_task, flow=self)
        # bulk_insert_task.set_upstream(create_table_task, flow=self)
