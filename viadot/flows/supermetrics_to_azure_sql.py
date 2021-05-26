from typing import Any, Dict, List

from prefect import Flow, Task, apply_map
from prefect.utilities import logging

from ..tasks import BlobFromCSV, CreateTableFromBlob, SupermetricsToCSV

logger = logging.get_logger(__name__)

supermetrics_to_csv_task = SupermetricsToCSV()
csv_to_blob_storage_task = BlobFromCSV()
blob_to_azure_sql_task = CreateTableFromBlob()


class SupermetricsToAzureSQL(Flow):
    def __init__(
        self,
        name: str,
        ds_id: str,
        ds_accounts: List[str],
        ds_segments: List[str],
        ds_user: str,
        fields: List[str],
        date_range_type: str = None,
        settings: Dict[str, Any] = None,
        filter: str = None,
        max_rows: int = 1000000,
        dtypes: Dict[str, Any] = None,
        blob_path: str = None,
        overwrite_blob: bool = True,
        table: str = None,
        schema: str = None,
        local_file_path: str = None,
        if_exists: str = "replace",  # this applies to the full CSV file, not per chunk
        if_empty: str = "warn",
        max_download_retries: int = 5,
        tags: List[str] = ["extract"],
        *args: List[any],
        **kwargs: Dict[str, Any]
    ):
        self.ds_id = ds_id
        self.ds_accounts = ds_accounts
        self.ds_segments = ds_segments
        self.ds_user = ds_user
        self.fields = fields
        self.date_range_type = date_range_type
        self.settings = settings
        self.filter = filter
        self.max_rows = max_rows
        self.local_file_path = local_file_path or self.slugify(name) + ".csv"
        self.blob_path = blob_path
        self.overwrite_blob = overwrite_blob
        self.table = table
        self.schema = schema
        self.dtypes = dtypes
        self.if_exists = if_exists
        self.if_empty = if_empty
        self.max_download_retries = max_download_retries
        self.tags = tags
        self.tasks = [
            supermetrics_to_csv_task,
            csv_to_blob_storage_task,
            blob_to_azure_sql_task,
        ]
        super().__init__(*args, name=name, **kwargs)
        self.gen_flow()

    @staticmethod
    def slugify(name):
        return name.replace(" ", "_").lower()

    def gen_supermetrics_task(self, ds_account: str, flow: Flow = None) -> Task:
        t = supermetrics_to_csv_task.bind(
            ds_id=self.ds_id,
            ds_account=ds_account,
            ds_segments=self.ds_segments,
            ds_user=self.ds_user,
            fields=self.fields,
            date_range_type=self.date_range_type,
            settings=self.settings,
            filter=self.filter,
            max_rows=self.max_rows,
            path=self.local_file_path,
            if_exists="append",
            if_empty=self.if_empty,
            max_retries=self.max_download_retries,
            flow=flow,
        )
        return t

    def gen_flow(self) -> Flow:
        supermetrics_downloads = apply_map(
            self.gen_supermetrics_task, self.ds_accounts, flow=self
        )
        csv_to_blob_storage_task.bind(
            from_path=self.local_file_path,
            to_path=self.blob_path,
            overwrite=self.overwrite_blob,
            flow=self,
        )
        blob_to_azure_sql_task.bind(
            blob_path=self.blob_path,
            schema=self.schema,
            table=self.table,
            dtypes=self.dtypes,
            if_exists=self.if_exists,
            flow=self,
        )

        csv_to_blob_storage_task.set_upstream(supermetrics_downloads, flow=self)
        blob_to_azure_sql_task.set_upstream(csv_to_blob_storage_task, flow=self)
