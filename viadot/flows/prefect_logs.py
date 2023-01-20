import prefect
import numpy as np
import pandas as pd
import datetime
from pandas import Timestamp
from typing import Literal

from prefect import Flow
from prefect.utilities import logging

from viadot.tasks.azure_data_lake import AzureDataLakeUpload
from viadot.task_utils import add_ingestion_metadata_task, df_to_parquet

logger = logging.get_logger()


class PrefectLogs(Flow):
    def __init__(
        self,
        name: str,
        query: str,
        scheduled_start_time: str = "yesterday",
        filter_type: Literal["_gte", "_gt", "_lte", "_lt"] = "_gte",
        local_file_path: str = None,
        adls_path: str = None,
        adls_sp_credentials_secret: str = None,
        vault_name: str = None,
        overwrite_adls: bool = True,
        timeout: int = 3600,
        *args,
        **kwargs,
    ):
        """
        Flow for extracting all details about flow runs that are running on Prefect.

        Args:
            name (str): The name of the flow.
            query (str): Query to be executed in Prefect API.
            scheduled_start_time (str, optional): A parameter passed to the Prefect API query. Set as 'yesterday' or date in format ex. '2022-01-01'. Defaults to 'yesterday'.
            filter_type (Literal, optional): A comparison operator passed to the Prefect API query (_gte >=, _lte <=) that refers to the left or right boundary of date ranges
                for which flow extracts data. Defaults to _gte.
            local_file_path (str, optional): Local destination path. Defaults to None.
            adls_path (str, optional): Azure Data Lake destination catalog or file path. Defaults to None.
            adls_sp_credentials_secret (str, optional): The name of the Azure Key Vault secret containing a dictionary with
                ACCOUNT_NAME and Service Principal credentials (TENANT_ID, CLIENT_ID, CLIENT_SECRET) for the Azure Data Lake.
                Defaults to None.
            vault_name (str, optional): The name of the vault from which to obtain the secrets. Defaults to None.
            overwrite_adls (bool, optional): Whether to overwrite the file in ADLS. Defaults to True.
            timeout(int, optional): The amount of time (in seconds) to wait while running this task before
                a timeout occurs. Defaults to 3600.

            Example query:
                {
                    project {
                        id
                        name
                        flows {
                                id
                                name
                                version
                                flow_runs(
                                    order_by: {end_time: desc}
                                    where: {_and:
                                        [
                                        {scheduled_start_time:{ %s: "%s" }},
                                        {state: {_neq: "Scheduled"}}
                                        ]
                                    }
                                    )
                                        {
                                        id
                                        scheduled_start_time
                                        start_time
                                        end_time
                                        state
                                        created_by_user_id
                                        }
                        }
                    }
                }
        """

        self.name = name
        self.query = query
        self.scheduled_start_time = scheduled_start_time
        self.filter_type = filter_type
        self.local_file_path = local_file_path
        self.adls_path = adls_path
        self.vault_name = vault_name
        self.overwrite_adls = overwrite_adls
        self.timeout = timeout
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
        Function returns cleaned date/time from timestamp in string format (ex. "2022-01-01" or "01:23:88").
        Args:
            value_type (Literal["time", "date"], optional): "date" or "time" type extracted from Prefect Timestamp
            value (Timestamp): Timestamp value from Prefect.
        Return:
            str: date (ex. "2022-01-01") or time (ex. "01:23:88") from the Timestamp in string format
        """
        try:
            if value is not None and value_type == "date":
                return value.split("T")[0]
            elif value is not None and value_type == "time":
                return (value.split("T")[1])[:8]
        except:
            pass

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

        query = self.query % (
            self.filter_type,
            self.scheduled_start_time,
        )

        client = prefect.Client()
        flow_runs = client.graphql(query)

        df = pd.json_normalize(
            data=flow_runs["data"]["project"],
            record_path=["flows"],
            record_prefix="flow.",
            meta=["name"],
            meta_prefix="project.",
        )

        df = df.dropna(subset=["flow.flow_runs"])
        df = df.explode("flow.flow_runs")
        df = pd.concat(
            [
                df.drop(["flow.flow_runs"], axis=1),
                df["flow.flow_runs"].apply(pd.Series),
            ],
            axis=1,
        )
        df = df.drop([0], axis=1).reset_index()

        df["start_date"] = [
            self.get_formatted_value_from_timestamp("date", df["start_time"][x])
            for x in range(df.shape[0])
        ]

        df["is_run_automatically"] = [
            self.check_if_run_authomatically(df["created_by_user_id"][x])
            for x in range(df.shape[0])
        ]

        df["scheduled_start_time_clean"] = [
            self.get_formatted_value_from_timestamp(
                "time", df["scheduled_start_time"][x]
            )
            if df["scheduled_start_time"][x] is not None
            else None
            for x in range(df.shape[0])
        ]

        df["start_time_clean"] = [
            self.get_formatted_value_from_timestamp("time", df["start_time"][x])
            if df["start_time"][x] is not None
            else None
            for x in range(df.shape[0])
        ]

        df["end_time_clean"] = [
            self.get_formatted_value_from_timestamp("time", df["end_time"][x])
            if df["end_time"][x] is not None
            else None
            for x in range(df.shape[0])
        ]

        df["time_delta"] = np.NaN

        for x in range(df.shape[0]):
            if (
                df["end_time_clean"][x] is not None
                and df["start_time_clean"][x] is not None
            ):
                df["time_delta"][x] = str(
                    datetime.timedelta(
                        seconds=(
                            datetime.datetime.strptime(
                                df["end_time_clean"][x], "%H:%M:%S"
                            )
                            - datetime.datetime.strptime(
                                df["start_time_clean"][x], "%H:%M:%S"
                            )
                        ).seconds
                    )
                )

        df["time_delta_delay"] = np.NaN

        for x in range(df.shape[0]):
            if (
                df["scheduled_start_time_clean"][x] is not None
                and df["end_time_clean"][x] is not None
            ):
                df["time_delta_delay"][x] = str(
                    datetime.timedelta(
                        seconds=(
                            datetime.datetime.strptime(
                                df["end_time_clean"][x], "%H:%M:%S"
                            )
                            - datetime.datetime.strptime(
                                df["scheduled_start_time_clean"][x], "%H:%M:%S"
                            )
                        ).seconds
                    )
                )

        df = add_ingestion_metadata_task.run(df.drop(["index"], axis=1))

        df_to_file = df_to_parquet.bind(
            df,
            path=self.local_file_path,
            flow=self,
        )

        azure_dl_upload_task = AzureDataLakeUpload(timeout=self.timeout)
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
