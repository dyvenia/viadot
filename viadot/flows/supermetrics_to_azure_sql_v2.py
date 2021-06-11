from typing import Any, Dict, List, Union

from prefect import Flow, Task, apply_map
from prefect.utilities import logging

from ..tasks import SupermetricsToCSV, AzureDataLakeUpload, AzureSQLBulkInsert

logger = logging.get_logger(__name__)

supermetrics_to_csv_task = SupermetricsToCSV()
csv_to_adls_task = AzureDataLakeUpload()
bulk_insert_task = AzureSQLBulkInsert()


class SupermetricsToAzureSQLv2(Flow):
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
        local_file_path: str = None,
        blob_path: str = None,
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
        # SupermetricsToCSV
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

        # AzureDataLakeUpload
        self.local_file_path = local_file_path or self.slugify(name) + ".csv"
        self.blob_path = blob_path
        self.sep = sep
        self.overwrite_adls = overwrite_adls
        self.if_empty = if_empty
        self.adls_sp_credentials_secret = adls_sp_credentials_secret

        # AzureSQLBulkInsert
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

        self.tasks = [
            supermetrics_to_csv_task,
            csv_to_adls_task,
            bulk_insert_task,
        ]
        super().__init__(*args, name=name, **kwargs)
        self.gen_flow()

    @staticmethod
    def slugify(name):
        return name.replace(" ", "_").lower()

    def gen_supermetrics_task(
        self, ds_accounts: Union[str, List[str]], flow: Flow = None
    ) -> Task:
        t = supermetrics_to_csv_task.bind(
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
            path=self.local_file_path,
            if_exists="append",
            if_empty=self.if_empty,
            max_retries=self.max_download_retries,
            timeout=self.supermetrics_task_timeout,
            sep=self.sep,
            flow=flow,
        )
        return t

    def gen_flow(self) -> Flow:
        if self.parallel:
            # generate a separate task for each account
            supermetrics_downloads = apply_map(
                self.gen_supermetrics_task, self.ds_accounts, flow=self
            )
        else:
            supermetrics_downloads = self.gen_supermetrics_task(
                ds_accounts=self.ds_accounts, flow=self
            )

        csv_to_adls_task.bind(
            from_path=self.local_file_path,
            to_path=self.blob_path,
            overwrite=self.overwrite_adls,
            sp_credentials_secret=self.adls_sp_credentials_secret,
            vault_name=self.vault_name,
            flow=self,
        )
        bulk_insert_task.bind(
            from_path=self.blob_path,
            schema=self.schema,
            table=self.table,
            dtypes=self.dtypes,
            sep=self.sep,
            if_exists=self.if_exists,
            credentials_secret=self.sqldb_credentials_secret,
            # vault_name=self.vault_name,  # add when KV support is added
            flow=self,
        )

        csv_to_adls_task.set_upstream(supermetrics_downloads, flow=self)
        bulk_insert_task.set_upstream(csv_to_adls_task, flow=self)
