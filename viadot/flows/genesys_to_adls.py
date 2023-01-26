from typing import Any, Dict, List, Literal

import pandas as pd
from prefect import Flow, task

from viadot.tasks import AzureDataLakeUpload
from viadot.tasks.genesys import GenesysToCSV, GenesysToDF
from viadot.task_utils import (
    add_ingestion_metadata_task,
    df_to_csv,
    df_to_parquet,
    adls_bulk_upload,
)


@task
def add_timestamp(files_names: List = None, sep: str = None) -> None:
    """Add new column _viadot_downloaded_at_utc into every genesys file.

    Args:
        files_names (List, optional): All file names of downloaded files. Defaults to None.
        sep (str, optional): Separator in csv file. Defaults to None.
    """
    for file in files_names:
        df = pd.read_csv(file, sep=sep)
        df_updated = add_ingestion_metadata_task.run(df)
        df_updated.to_csv(file, index=False, sep=sep)


class GenesysToADLS(Flow):
    def __init__(
        self,
        name: str,
        view_type: str = "queue_performance_detail_view",
        view_type_time_sleep: int = 80,
        media_type_list: List[str] = None,
        queueIds_list: List[str] = None,
        data_to_post_str: str = None,
        start_date: str = None,
        end_date: str = None,
        days_interval: int = 1,
        sep: str = "\t",
        environment: str = None,
        schedule_id: str = None,
        report_url: str = None,
        report_columns: List[str] = None,
        local_file_path: str = None,
        adls_file_path: str = None,
        overwrite_adls: bool = True,
        adls_sp_credentials_secret: str = None,
        credentials_genesys: Dict[str, Any] = None,
        timeout: int = 3600,
        *args: List[any],
        **kwargs: Dict[str, Any]
    ):
        """
        Genesys flow that generates CSV files and upload them to ADLS.

        Args:
            name (str): The name of the Flow.
            view_type (str, optional): The type of view export job to be created. Defaults to "queue_performance_detail_view".
            view_type_time_sleep (int, optional): Waiting time to retrieve data from Genesys API. Defaults to 80.
            media_type_list (List[str], optional): List of specific media types. Defaults to None.
            queueIds_list (List[str], optional): List of specific queues ids. Defaults to None.
            data_to_post_str (str, optional): String template to generate json body. Defaults to None.
            start_date (str, optional): Start date of the report. Defaults to None.
            end_date (str, optional): End date of the report. Defaults to None.
            days_interval (int, optional): How many days report should include. Defaults to 1.
            sep (str, optional): Separator in csv file. Defaults to "\t".
            environment (str, optional): Adress of host server. Defaults to None than will be used enviroment
            from credentials.
            schedule_id (str, optional): The ID of report. Defaults to None.
            report_url (str, optional): The url of report generated in json response. Defaults to None.
            report_columns (List[str], optional): List of exisiting column in report. Defaults to None.
            local_file_path (str, optional): The local path from which to upload the file(s). Defaults to None.
            adls_file_path (str, optional): The destination path at ADLS. Defaults to None.
            overwrite_adls (bool, optional): Whether to overwrite files in the data lake. Defaults to True.
            adls_sp_credentials_secret (str, optional): The name of the Azure Key Vault secret containing a dictionary with
            ACCOUNT_NAME and Service Principal credentials (TENANT_ID, CLIENT_ID, CLIENT_SECRET). Defaults to None.
            credentials(dict, optional): Credentials for the genesys api. Defaults to None.
            timeout(int, optional): The amount of time (in seconds) to wait while running this task before
                a timeout occurs. Defaults to 3600.
        """
        # GenesysToCSV
        self.flow_name = name
        self.view_type = view_type
        self.view_type_time_sleep = view_type_time_sleep
        self.media_type_list = media_type_list
        self.queueIds_list = queueIds_list
        self.data_to_post = data_to_post_str
        self.environment = environment
        self.schedule_id = schedule_id
        self.report_url = report_url
        self.report_columns = report_columns
        self.start_date = start_date
        self.end_date = end_date
        self.days_interval = days_interval
        self.sep = sep
        self.timeout = timeout

        # AzureDataLake
        self.local_file_path = local_file_path
        self.adls_file_path = adls_file_path
        self.overwrite_adls = overwrite_adls
        self.adls_sp_credentials_secret = adls_sp_credentials_secret
        self.credentials_genesys = credentials_genesys

        super().__init__(*args, name=self.flow_name, **kwargs)

        self.gen_flow()

    def gen_flow(self) -> Flow:

        to_csv = GenesysToCSV(timeout=self.timeout)

        if self.view_type == "queue_performance_detail_view":
            file_names = to_csv.bind(
                view_type=self.view_type,
                view_type_time_sleep=self.view_type_time_sleep,
                media_type_list=self.media_type_list,
                queueIds_list=self.queueIds_list,
                data_to_post_str=self.data_to_post,
                start_date=self.start_date,
                end_date=self.end_date,
                days_interval=self.days_interval,
                environment=self.environment,
                credentials_genesys=self.credentials_genesys,
                flow=self,
            )
        elif self.view_type in [
            "agent_performance_summary_view",
            "agent_status_summary_view",
        ]:
            file_names = to_csv.bind(
                view_type=self.view_type,
                view_type_time_sleep=self.view_type_time_sleep,
                media_type_list=self.media_type_list,
                queueIds_list=[""],
                data_to_post_str=self.data_to_post,
                start_date=self.start_date,
                environment=self.environment,
                credentials_genesys=self.credentials_genesys,
                flow=self,
            )

        add_timestamp.bind(file_names, sep=self.sep, flow=self)

        adls_bulk_upload.bind(
            file_names=file_names,
            adls_file_path=self.adls_file_path,
            adls_sp_credentials_secret=self.adls_sp_credentials_secret,
            timeout=self.timeout,
            flow=self,
        )

        add_timestamp.set_upstream(file_names, flow=self)
        adls_bulk_upload.set_upstream(add_timestamp, flow=self)


class GenesysReportToADLS(Flow):
    def __init__(
        self,
        name: str,
        columns: List[str] = None,
        vault_name: str = None,
        local_file_path: str = None,
        output_file_extension: str = ".csv",
        sep: str = "\t",
        adls_file_path: str = None,
        if_exists: Literal["replace", "append", "delete"] = "replace",
        overwrite_adls: bool = True,
        adls_sp_credentials_secret: str = None,
        credentials_secret: str = None,
        schedule_id: str = None,
        timeout: int = 3600,
        *args: List[any],
        **kwargs: Dict[str, Any]
    ):
        """
        Flow for downloading data from genesys API to Azure Data Lake.

        Args:
            name (str): The name of the flow.
            columns (List[str], optional): The report fields. Defaults to None.
            vault_name (str, optional): The name of the vault from which to obtain the secrets. Defaults to None.
            local_file_path (str, optional): Local destination path. Defaults to None.
            output_file_extension (str, optional): Output file extension - to allow selection of.csv for data
            which is not easy to handle with parquet. Defaults to ".csv".
            sep (str, optional): The separator used in csv. Defaults to "\t".
            adls_file_path (str, optional): Azure Data Lake destination file path. Defaults to None.
            if_exists (str, optional): What to do if the file exists. Defaults to "replace".
            overwrite_adls (bool, optional): Whether to overwrite files in the lake. Defaults to True.
            adls_sp_credentials_secret (str, optional): The name of the Azure Key Vault secret containing a dictionary with
            ACCOUNT_NAME and Service Principal credentials (TENANT_ID, CLIENT_ID, CLIENT_SECRET) for the Azure Data Lake.
            Defaults to None.
            credentials_secret (str, optional): The name of the Azure Key Vault secret for Genesys project. Defaults to None.
            schedule_id (str, optional): ID of the schedule report job. Defaults to None.
            timeout(int, optional): The amount of time (in seconds) to wait while running this task before
                a timeout occurs. Defaults to 3600.
        """

        self.name = name
        self.columns = columns
        self.vault_name = vault_name
        self.local_file_path = local_file_path
        self.output_file_extension = output_file_extension
        self.sep = sep
        self.if_exists = if_exists
        self.adls_file_path = adls_file_path
        self.overwrite_adls = overwrite_adls
        self.adls_sp_credentials_secret = adls_sp_credentials_secret
        self.credentials_secret = credentials_secret
        self.if_exsists = if_exists
        self.schedule_id = schedule_id
        self.timeout = timeout

        super().__init__(*args, name=name, **kwargs)

        self.gen_flow()

    def gen_flow(self) -> Flow:

        genesys_report = GenesysToDF(timeout=self.timeout)

        df = genesys_report.bind(
            report_columns=self.columns,
            schedule_id=self.schedule_id,
            credentials_genesys=self.credentials_secret,
            flow=self,
        )
        df_with_metadata = add_ingestion_metadata_task.bind(df, flow=self)

        if self.output_file_extension == ".parquet":
            df_to_file = df_to_parquet.bind(
                df=df_with_metadata,
                path=self.local_file_path,
                if_exists=self.if_exsists,
                flow=self,
            )
        else:
            df_to_file = df_to_csv.bind(
                df=df_with_metadata,
                path=self.local_file_path,
                sep=self.sep,
                if_exists=self.if_exsists,
                flow=self,
            )

        file_to_adls_task = AzureDataLakeUpload(timeout=self.timeout)
        file_to_adls_task.bind(
            from_path=self.local_file_path,
            to_path=self.adls_file_path,
            overwrite=self.overwrite_adls,
            sp_credentials_secret=self.adls_sp_credentials_secret,
            flow=self,
        )

        df_with_metadata.set_upstream(df, flow=self)
        df_to_file.set_upstream(df_with_metadata, flow=self)
        file_to_adls_task.set_upstream(df_to_file, flow=self)
