import os
import pendulum
import json
from typing import Any, Dict, List, Union

from visions.functional import infer_type
from visions.typesets.complete_set import CompleteSet

import pandas as pd
from prefect import Flow, Task, apply_map, task
from prefect.utilities import logging
from prefect.backend import set_key_value

from pathlib import Path

from ..task_utils import add_ingestion_metadata_task
from ..tasks import (
    AzureDataLakeUpload,
    DownloadGitHubFile,
    RunGreatExpectationsValidation,
    SupermetricsToDF,
)

logger = logging.get_logger(__name__)

supermetrics_to_df_task = SupermetricsToDF()
download_github_file_task = DownloadGitHubFile()
validation_task = RunGreatExpectationsValidation()
parquet_to_adls_task = AzureDataLakeUpload()
json_to_adls_task = AzureDataLakeUpload()


@task
def get_data_types(df: pd.DataFrame) -> dict:
    df.dtypes.to_dict()


@task
def union_dfs_task(dfs: List[pd.DataFrame]):
    return pd.concat(dfs, ignore_index=True)


@task
def df_get_data_types_task(df):
    typeset = CompleteSet()
    dtypes = infer_type(df, typeset)
    dtypes_dict = {k: str(v) for k, v in dtypes.items()}
    return dtypes_dict


@task
def dtypes_to_json_task(dtypes_dict, local_json_path: str):
    with open(local_json_path, "w") as fp:
        json.dump(dtypes_dict, fp)


@task
def df_to_parquet_task(df, path: str, if_exists: str = "replace"):
    if if_exists == "append":
        if os.path.isfile(path):
            parquet_df = pd.read_parquet(path)
            out_df = pd.concat([parquet_df, df])
        else:
            out_df = df
    elif if_exists == "replace":
        out_df = df
    out_df.to_parquet(path, index=False)


class SupermetricsToADLS(Flow):
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
        expectation_suite_name: str = "failure",
        local_file_path: str = None,
        adls_dir_path: str = None,
        overwrite_adls: bool = True,
        if_empty: str = "warn",
        if_exists: str = "replace",
        adls_sp_credentials_secret: str = None,
        max_download_retries: int = 5,
        supermetrics_task_timeout: int = 60 * 30,
        parallel: bool = True,
        tags: List[str] = ["extract"],
        vault_name: str = None,
        *args: List[any],
        **kwargs: Dict[str, Any],
    ):
        """
        Flow for downloading data from different marketing APIs to a local CSV
        using Supermetrics API, then uploading it to Azure Data Lake.

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
            expectation_suite_name (str, optional): The name of the expectation suite. Defaults to "failure".
            Currently, only GitHub URLs are supported. Defaults to None.
            local_file_path (str, optional): Local destination path. Defaults to None.
            adls_dir_path (str, optional): Azure Data Lake destination folder/catalog path. Defaults to None.
            sep (str, optional): The separator to use in the CSV. Defaults to "\t".
            overwrite_adls (bool, optional): Whether to overwrite the file in ADLS. Defaults to True.
            if_empty (str, optional): What to do if the Supermetrics query returns no data. Defaults to "warn".
            adls_sp_credentials_secret (str, optional): The name of the Azure Key Vault secret containing a dictionary with
            ACCOUNT_NAME and Service Principal credentials (TENANT_ID, CLIENT_ID, CLIENT_SECRET) for the Azure Data Lake.
            Defaults to None.
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
        self.if_exists = if_exists

        # RunGreatExpectationsValidation

        # AzureDataLakeUpload

        self.local_file_path = local_file_path or self.slugify(name) + ".parquet"
        self.local_json_path = self.slugify(name) + ".json"
        self.now = str(pendulum.now("utc"))
        self.adls_dir_path = adls_dir_path
        self.adls_file_path = os.path.join(adls_dir_path, self.now + ".parquet")
        self.adls_schema_file_dir_file = os.path.join(
            adls_dir_path, "schema", self.now + ".json"
        )
        self.overwrite_adls = overwrite_adls
        self.if_empty = if_empty
        self.adls_sp_credentials_secret = adls_sp_credentials_secret

        # Global
        self.max_download_retries = max_download_retries
        self.supermetrics_task_timeout = supermetrics_task_timeout
        self.parallel = parallel
        self.tags = tags
        self.vault_name = vault_name
        self.flow_dir_path = self._get_flow_dir_path()

        super().__init__(*args, name=name, **kwargs)

        # DownloadGitHubFile (download expectations)
        self.expectation_suite_name = expectation_suite_name
        self.expectation_suite_file_name = expectation_suite_name + ".json"
        # self.expectation_suite_path = self._get_expectation_suite_path()

        self.gen_flow()

    @staticmethod
    def slugify(name):
        return name.replace(" ", "_").lower()

    def _get_flow_dir_path(self):
        try:
            flow_dir_path = Path(__file__).resolve().parent
        except NameError:
            # when flow is ran from a Jupyter notebook
            flow_dir_path = os.path.abspath(".")
        return flow_dir_path

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
            df = union_dfs_task.bind(dfs, flow=self)
        else:
            df = self.gen_supermetrics_task(ds_accounts=self.ds_accounts, flow=self)

        validation = validation_task.bind(
            df=df,
            expectations_path=self.flow_dir_path,
            expectation_suite_name=self.expectation_suite_name,
            flow=self,
        )

        df_with_metadata = add_ingestion_metadata_task(df, flow=self)

        df_to_parquet = df_to_parquet_task.bind(
            df=df_with_metadata,
            path=self.local_file_path,
            if_exists=self.if_exists,
            flow=self,
        )

        parquet_to_adls_task.bind(
            from_path=self.local_file_path,
            to_path=self.adls_file_path,
            overwrite=self.overwrite_adls,
            sp_credentials_secret=self.adls_sp_credentials_secret,
            vault_name=self.vault_name,
            flow=self,
        )
        dtypes_dict = df_get_data_types_task.bind(df_with_metadata, flow=self)
        dtypes_to_json_task.bind(
            dtypes_dict=dtypes_dict, local_json_path=self.local_json_path, flow=self
        )
        json_to_adls_task.bind(
            from_path=self.local_json_path,
            to_path=self.adls_schema_file_dir_file,
            overwrite=self.overwrite_adls,
            sp_credentials_secret=self.adls_sp_credentials_secret,
            vault_name=self.vault_name,
            flow=self,
        )
        df_with_metadata.set_upstream(validation, flow=self)
        parquet_to_adls_task.set_upstream(df_to_parquet, flow=self)
        json_to_adls_task.set_upstream(dtypes_to_json_task, flow=self)
        set_key_value(key=self.adls_dir_path, value=self.adls_file_path)
