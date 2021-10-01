import json
import os
import shutil
from pathlib import Path
from typing import Any, Dict, List, Union

import pandas as pd
import pendulum
import prefect
from prefect import Flow, Task, apply_map, task
from prefect.backend import set_key_value
from prefect.tasks.secrets import PrefectSecret
from prefect.utilities import logging
from visions.functional import infer_type
from visions.typesets.complete_set import CompleteSet

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
file_to_adls_task = AzureDataLakeUpload()
json_to_adls_task = AzureDataLakeUpload()


@task
def write_to_json(dict_, path):

    logger = prefect.context.get("logger")

    if os.path.isfile(path):
        logger.warning(f"File {path} already exists. Overwriting...")
    else:
        logger.debug(f"Writing to {path}...")

    # create parent directories if they don't exist
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    with open(path, mode="w") as f:
        json.dump(dict_, f)

    logger.debug(f"Successfully wrote to {path}.")


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


@task
def df_to_csv_task(df, path: str, if_exists: str = "replace"):
    if if_exists == "append":
        if os.path.isfile(path):
            csv_df = pd.read_csv(path)
            out_df = pd.concat([csv_df, df])
        else:
            out_df = df
    elif if_exists == "replace":
        out_df = df
    out_df.to_csv(path, index=False)


@task
def cleanup_validation_clutter(expectations_path):
    ge_project_path = Path(expectations_path).parent
    shutil.rmtree(ge_project_path)


class SupermetricsToADLS(Flow):
    def __init__(
        self,
        name: str,
        ds_id: str,
        ds_accounts: List[str],
        fields: List[str],
        ds_user: str = None,
        ds_segments: List[str] = None,
        date_range_type: str = None,
        start_date: str = None,
        end_date: str = None,
        settings: Dict[str, Any] = None,
        filter: str = None,
        max_rows: int = 1000000,
        max_columns: int = None,
        order_columns: str = None,
        expectation_suite: dict = None,
        evaluation_parameters: dict = None,
        keep_validation_output: bool = False,
        output_file_extension: str = ".parquet",
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
            start_date (str, optional): A query paramter to pass start date to the date range filter. Defaults to None.
            end_date (str, optional): A query paramter to pass end date to the date range filter. Defaults to None.
            settings (Dict[str, Any], optional): A query parameter passed to the SupermetricsToCSV task. Defaults to None.
            filter (str, optional): A query parameter passed to the SupermetricsToCSV task. Defaults to None.
            max_rows (int, optional): A query parameter passed to the SupermetricsToCSV task. Defaults to 1000000.
            max_columns (int, optional): A query parameter passed to the SupermetricsToCSV task. Defaults to None.
            order_columns (str, optional): A query parameter passed to the SupermetricsToCSV task. Defaults to None.
            expectation_suite (dict, optional): The Great Expectations suite used to valiaate the data. Defaults to None.
            evaluation_parameters (str, optional): A dictionary containing evaluation parameters for the validation. Defaults to None.
            keep_validation_output (bool, optional): Whether to keep the output files generated by the Great Expectations task. Defaults to False.
            Currently, only GitHub URLs are supported. Defaults to None.
            local_file_path (str, optional): Local destination path. Defaults to None.
            output_file_extension (str, optional): Output file extension - to allow selection of .csv for data which is not easy to handle with parquet. Defaults to ".parquet"..
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
        if not ds_user:
            try:
                ds_user = PrefectSecret("SUPERMETRICS_DEFAULT_USER").run()
            except ValueError as e:
                msg = "Neither 'ds_user' parameter nor 'SUPERMETRICS_DEFAULT_USER' secret were not specified"
                raise ValueError(msg) from e

        # SupermetricsToDF
        self.ds_id = ds_id
        self.ds_accounts = ds_accounts
        self.ds_segments = ds_segments
        self.ds_user = ds_user
        self.fields = fields
        self.date_range_type = date_range_type
        self.start_date = start_date
        self.end_date = end_date
        self.settings = settings
        self.filter = filter
        self.max_rows = max_rows
        self.max_columns = max_columns
        self.order_columns = order_columns
        self.if_exists = if_exists
        self.output_file_extension = output_file_extension

        # RunGreatExpectationsValidation
        self.expectation_suite = expectation_suite
        self.expectations_path = "/home/viadot/tmp/expectations"
        self.expectation_suite_name = expectation_suite["expectation_suite_name"]
        self.evaluation_parameters = evaluation_parameters
        self.keep_validation_output = keep_validation_output

        # AzureDataLakeUpload
        self.local_file_path = (
            local_file_path or self.slugify(name) + self.output_file_extension
        )
        self.local_json_path = self.slugify(name) + ".json"
        self.now = str(pendulum.now("utc"))
        self.adls_dir_path = adls_dir_path
        self.adls_file_path = os.path.join(
            adls_dir_path, self.now + self.output_file_extension
        )
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

        super().__init__(*args, name=name, **kwargs)

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
            start_date=self.start_date,
            end_date=self.end_date,
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

        write_json = write_to_json.bind(
            dict_=self.expectation_suite,
            path=os.path.join(
                self.expectations_path, self.expectation_suite_name + ".json"
            ),
            flow=self,
        )

        validation = validation_task.bind(
            df=df,
            expectations_path=self.expectations_path,
            expectation_suite_name=self.expectation_suite_name,
            evaluation_parameters=self.evaluation_parameters,
            keep_output=self.keep_validation_output,
            flow=self,
        )

        if not self.keep_validation_output:
            validation_cleanup = cleanup_validation_clutter.bind(
                expectations_path=self.expectations_path, flow=self
            )
            validation_cleanup.set_upstream(validation, flow=self)
            validation_upstream = validation_cleanup
        else:
            validation_upstream = validation

        df_with_metadata = add_ingestion_metadata_task.bind(df, flow=self)

        if self.output_file_extension == ".parquet":
            df_to_file = df_to_parquet_task.bind(
                df=df_with_metadata,
                path=self.local_file_path,
                if_exists=self.if_exists,
                flow=self,
            )
        else:
            df_to_file = df_to_csv_task.bind(
                df=df_with_metadata,
                path=self.local_file_path,
                if_exists=self.if_exists,
                flow=self,
            )

        file_to_adls_task.bind(
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

        write_json.set_upstream(df, flow=self)
        validation.set_upstream(write_json, flow=self)
        df_with_metadata.set_upstream(validation_upstream, flow=self)
        file_to_adls_task.set_upstream(df_to_file, flow=self)
        json_to_adls_task.set_upstream(dtypes_to_json_task, flow=self)
        set_key_value(key=self.adls_dir_path, value=self.adls_file_path)
