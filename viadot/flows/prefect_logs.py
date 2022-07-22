from pandas import Timestamp
from typing import Literal
import prefect
import pandas as pd
import datetime

from prefect import Flow
from prefect.utilities import logging

from viadot.tasks.azure_data_lake import AzureDataLakeUpload
from viadot.task_utils import add_ingestion_metadata_task, df_to_parquet

logger = logging.get_logger()
azure_dl_upload_task = AzureDataLakeUpload()


class PrefectLogs(Flow):
    def __init__(
        self,
        name: str,
        scheduled_start_time: str = "yesterday",
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
            scheduled_start_time (str, optional): A parameter passed to the Prefect API query. Defaults to 'yesterday'.
            filter_type (Literal, optional): A comparison operator passed to the Prefect API query (_gte >=, _lte <=) that refers to the left or right boundary of date ranges
                for which flow extracts data. Defaults to _gte.
            local_file_path (str, optional): Local destination path. Defaults to None.
            adls_path (str, optional): Azure Data Lake destination catalog or file path. Defaults to None.
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

        if scheduled_start_time == "yesterday":
            self.scheduled_start_time = datetime.date.today() - datetime.timedelta(
                days=1
            )
        else:
            self.scheduled_start_time = scheduled_start_time

        super().__init__(
            name="prefect_extract_flow_logs",
            *args,
            **kwargs,
        )

        self.gen_flow()

    def get_formatted_value_from_timestamp(
        self, value_type: Literal["time", "date"], value: Timestamp
    ) -> str:
        """
        Function returns cleaned date/time from timestamp in string format (ex. "2022-01-01").
        Args:
            value_type (Literal["time", "date"], optional): "date" or "time" type extracted from Prefect Timestamp
            value (Timestamp): Timestamp value from Prefect.
        Return:
            str: date (ex. "2022-01-01") or time (ex. "13:10") from the Timestamp in string format
        """
        if value is not None and value_type == "date":
            return value.split("T")[0]
        elif value is not None and value_type == "time":
            return (value.split("T")[1])[:5]

    def check_if_run_authomatically(self, value: str) -> bool:
        """
        Function checks if flow has been run automatically by scheduler or triggered manually.
        Args:
            value (str): User ID created by Prefect.
            - ID = "09720c91-a99c-4f72-b7b5-3c061c83408b" refers to Prefect scheduler
            - other IDs  = tells that flow has been run manually
        Return: (bool) Boolean value True/False.
        """
        if value == "09720c91-a99c-4f72-b7b5-3c061c83408b":  # ID of a Prefect scheduler
            return True
        else:
            return False

    def gen_flow(self) -> Flow:

        query = """
                {
                  project {
                        id
                        name
                      }
                  flow {  
                        project_id
                        name
                        version
                        flow_runs(
                          order_by: {end_time: desc}
                          where: {_and: 
                            [
                              {end_time: {_is_null: false}},
                              {scheduled_start_time:{ %s: "%s" }}
                            ]
                          }
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

        projects_details = {}
        for x in flow_runs["data"]["project"]:
            projects_details[x["id"]] = x["name"]

        df = pd.json_normalize(
            data=flow_runs["data"]["flow"],
            record_path=["flow_runs"],
            meta=["name", "project_id", "version"],
        )

        df["start_time_date"] = [
            self.get_formatted_value_from_timestamp("date", df["start_time"][x])
            for x in range(df.shape[0])
        ]
        df["start_time_time"] = [
            self.get_formatted_value_from_timestamp("time", df["start_time"][x])
            for x in range(df.shape[0])
        ]
        df["is_run_automatically"] = [
            self.check_if_run_authomatically(df["created_by_user_id"][x])
            for x in range(df.shape[0])
        ]

        df["project_name"] = df["project_id"].map(projects_details)
        df = df.loc[
            :,
            [
                "id",
                "name",
                "version",
                "scheduled_start_time",
                "start_time",
                "start_time_date",
                "start_time_time",
                "end_time",
                "state",
                "created_by_user_id",
                "is_run_automatically",
                "project_id",
                "project_name",
            ],
        ]

        df = add_ingestion_metadata_task.run(df)

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
