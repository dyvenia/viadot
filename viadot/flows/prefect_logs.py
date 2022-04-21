from typing import Literal
import prefect
import pandas as pd
import datetime

from prefect import Flow, task, Task
from prefect.utilities import logging

from viadot.tasks.azure_data_lake import AzureDataLakeUpload

from viadot.task_utils import add_ingestion_metadata_task, df_to_parquet

logger = logging.get_logger()
azure_dl_upload_task = AzureDataLakeUpload()


class PrefectLogs(Flow):
    def __init__(
        self,
        name: str,
        scheduled_start_time: str = None,
        filter_type: Literal["_gte", "_gt", "_lte", "_lt"] = "_gte",
        local_file_path: str = None,
        adls_path: str = None,
        adls_sp_credentials_secret: str = None,
        vault_name: str = None,
        overwrite_adls: bool = True,
        *args,
        **kwargs,
    ):
        """
        Flow for extracting all details about flow runs that are running on Prefect.

        Args:
            name (str): The name of the flow.
            scheduled_start_time (str, optional): A parameter passed to the Prefect API query. Defaults to None.
            filter_type (Literal, optional): A comparison operator passed to the Prefect API query (_gte >=, _lte <=)  Defaults to _gte.
            local_file_path (str, optional): Local destination path. Defaults to None.
            adls_path (str, optional): Azure Data Lake destination folder/catalog path. Defaults to None.
            adls_sp_credentials_secret (str, optional): The name of the Azure Key Vault secret containing a dictionary with
                ACCOUNT_NAME and Service Principal credentials (TENANT_ID, CLIENT_ID, CLIENT_SECRET) for the Azure Data Lake.
                Defaults to None.
            vault_name (str, optional): The name of the vault from which to obtain the secrets. Defaults to None.
            overwrite_adls (bool, optional): Whether to overwrite the file in ADLS. Defaults to True.

        """

        self.name = name
        self.scheduled_start_time = scheduled_start_time
        self.filter_type = filter_type
        self.local_file_path = local_file_path
        self.adls_path = adls_path
        self.vault_name = vault_name
        self.overwrite_adls = overwrite_adls
        self.adls_sp_credentials_secret = adls_sp_credentials_secret

        super().__init__(
            name="prefect_extract_flows_logs",
            *args,
            **kwargs,
        )

        self.gen_flow()

    def get_formatted_date_from_timestamp(self, col_value):
        if col_value is not None:
            return col_value.split("T")[0]

    def gen_flow(self) -> Flow:

        query = """
            {
                flow{
                        name
                        version
                        flow_runs(
                            order_by: {end_time: desc}
                            where: { _and: [
                                  {end_time:{ _is_null:false }},
                                  {scheduled_start_time: {%s: "%s"} }
                            ] } 
                        ) 
                                {
                                    id
                                    end_time
                                    start_time
                                    state
                                    scheduled_start_time
                                    created_by_user_id
                                }  
                }
            }

        """ % (
            self.filter_type,
            self.scheduled_start_time,
        )

        client = prefect.Client()
        flow_runs = client.graphql(query)

        df = pd.json_normalize(
            data=flow_runs["data"]["flow"],
            record_path=["flow_runs"],
            meta=["name", "version"],
        )

        df = add_ingestion_metadata_task.run(df)
        date_column = df["start_time"]

        df.insert(
            3,
            "start_date",
            value=[
                self.get_formatted_date_from_timestamp(date_column[x])
                for x in range(df.shape[0])
            ],
        )

        df_to_file = df_to_parquet.bind(
            df,
            path=self.local_file_path,
            flow=self,
        )

        adls_upload = azure_dl_upload_task.bind(
            from_path=self.local_file_path,
            to_path=self.adls_path,
            overwrite=self.overwrite_adls,
            sp_credentials_secret=self.adls_sp_credentials_secret,
            vault_name=self.vault_name,
            flow=self,
        )

        df_to_file.set_upstream(df, flow=self)
        adls_upload.set_upstream(df_to_file, flow=self)
