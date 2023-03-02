import os
from typing import Any, Dict, List, Any

import pandas as pd
from prefect import Flow, task

from viadot.tasks.genesys import GenesysToCSV
from viadot.task_utils import (
    add_ingestion_metadata_task,
    adls_bulk_upload,
)


@task(timeout=3600)
def add_timestamp(files_names: List = None, path: str = "", sep: str = "\t") -> None:
    """Add new column _viadot_downloaded_at_utc into every genesys file.

    Args:
        files_names (List, optional): All file names of downloaded files. Defaults to "\t".
        path (str, optional): Relative path to the file. Defaults to empty string.
        sep (str, optional): Separator in csv file. Defaults to None.
    """
    for file in files_names:
        df = pd.read_csv(os.path.join(path, file), sep=sep)
        df_updated = add_ingestion_metadata_task.run(df)
        df_updated.to_csv(os.path.join(path, file), index=False, sep=sep)


class GenesysToADLS(Flow):
    def __init__(
        self,
        name: str,
        view_type: str = "queue_performance_detail_view",
        view_type_time_sleep: int = 80,
        post_data_list: List[str] = None,
        start_date: str = None,
        end_date: str = None,
        sep: str = "\t",
        environment: str = None,
        schedule_id: str = None,
        report_url: str = None,
        report_columns: List[str] = None,
        local_file_path: str = "",
        adls_file_path: str = None,
        overwrite_adls: bool = True,
        adls_sp_credentials_secret: str = None,
        credentials_genesys: Dict[str, Any] = None,
        timeout: int = 3600,
        *args: List[any],
        **kwargs: Dict[str, Any],
    ):
        """
        Genesys flow that generates CSV files and upload them to ADLS.

        Args:
            name (str): The name of the Flow.
            view_type (str, optional): The type of view export job to be created. Defaults to "queue_performance_detail_view".
            view_type_time_sleep (int, optional): Waiting time to retrieve data from Genesys API. Defaults to 80.
            post_data_list (List[str], optional): List of string templates to generate json body. Defaults to None.
                Example for only one POST:
                >>> post_data_list = '''[{
                >>>         "name": "AGENT_STATUS_DETAIL_VIEW",
                >>>         "timeZone": "UTC",
                >>>         "exportFormat": "CSV",
                >>>         "interval": ""2022-06-09T00:00:00/2022-06-10T00:00:00",
                >>>         "period": "PT30M",
                >>>         "viewType": "AGENT_STATUS_DETAIL_VIEW",
                >>>         "filter": {"userIds": ["aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"],},
                >>>         "read": True,
                >>>         "locale": "en-us",
                >>>         "hasFormatDurations": False,
                >>>         "hasSplitFilters": True,
                >>>         "excludeEmptyRows": True,
                >>>         "hasSummaryRow": False,
                >>>         "csvDelimiter": "COMMA",
                >>>         "hasCustomParticipantAttributes": True,
                >>>     }]'''
                If you need to add more POSTs in the same call, just add them to the list separated by a comma.
            start_date (str, optional): Start date of the report. Defaults to None.
            end_date (str, optional): End date of the report. Defaults to None.
            sep (str, optional): Separator in csv file. Defaults to "\t".
            environment (str, optional): Adress of host server. Defaults to None than will be used enviroment
                from credentials.
            schedule_id (str, optional): The ID of report. Defaults to None.
            report_url (str, optional): The url of report generated in json response. Defaults to None.
            report_columns (List[str], optional): List of exisiting column in report. Defaults to None.
            local_file_path (str, optional): The local path from which to upload the file(s). Defaults to "".
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
        self.post_data_list = post_data_list
        self.environment = environment
        self.schedule_id = schedule_id
        self.report_url = report_url
        self.report_columns = report_columns
        self.start_date = start_date
        self.end_date = end_date
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

        to_csv = GenesysToCSV(
            timeout=self.timeout, local_file_path=self.local_file_path
        )

        file_names = to_csv.bind(
            view_type=self.view_type,
            view_type_time_sleep=self.view_type_time_sleep,
            post_data_list=self.post_data_list,
            start_date=self.start_date,
            end_date=self.end_date,
            environment=self.environment,
            credentials_genesys=self.credentials_genesys,
            flow=self,
        )

        add_timestamp.bind(
            file_names, path=self.local_file_path, sep=self.sep, flow=self
        )

        adls_bulk_upload.bind(
            file_names=file_names,
            file_name_relative_path=self.local_file_path,
            adls_file_path=self.adls_file_path,
            adls_sp_credentials_secret=self.adls_sp_credentials_secret,
            timeout=self.timeout,
            flow=self,
        )

        add_timestamp.set_upstream(file_names, flow=self)
        adls_bulk_upload.set_upstream(add_timestamp, flow=self)
