from typing import Any, Dict, List

from prefect import Flow

from ..tasks import BlobFromCSV, CreateTableFromBlob, SupermetricsToCSV

supermetrics_to_csv_task = SupermetricsToCSV()
csv_to_blob_storage_task = BlobFromCSV()
blob_to_azure_sql_task = CreateTableFromBlob()


class SupermetricsToAzureSQL(Flow):
    def __init__(
        self,
        name: str,
        query: Dict[str, Any] = None,
        dtypes: Dict[str, Any] = None,
        blob_path: str = None,
        table: str = None,
        schema: str = None,
        local_file_path: str = None,
        if_exists: str = "replace",
        *args: List[any],
        **kwargs: Dict[str, Any]
    ):
        self.query = query
        self.dtypes = dtypes
        self.blob_path = blob_path
        self.table = table
        self.schema = schema
        self.local_file_path = local_file_path or "test.csv"
        self.if_exists = if_exists
        self.tasks = [
            supermetrics_to_csv_task,
            csv_to_blob_storage_task,
            blob_to_azure_sql_task,
        ]
        super().__init__(*args, name=name, **kwargs)
        self.gen_flow()

    def gen_flow(self):
        supermetrics_to_csv_task.bind(
            query=self.query, path=self.local_file_path, flow=self
        )
        csv_to_blob_storage_task.bind(
            from_path=self.local_file_path, to_path=self.blob_path, flow=self
        )
        blob_to_azure_sql_task.bind(
            blob_path=self.blob_path,
            schema=self.schema,
            table=self.table,
            dtypes=self.dtypes,
            if_exists=self.if_exists,
            flow=self,
        )

        csv_to_blob_storage_task.set_upstream(supermetrics_to_csv_task, flow=self)
        blob_to_azure_sql_task.set_upstream(csv_to_blob_storage_task, flow=self)
