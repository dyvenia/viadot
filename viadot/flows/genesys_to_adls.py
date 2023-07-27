import os
from typing import Any, Dict, List

import numpy as np
import pandas as pd
from prefect import Flow, task

from viadot.task_utils import add_ingestion_metadata_task, adls_bulk_upload
from viadot.tasks.genesys import GenesysToCSV


@task(timeout=3600)
def add_timestamp(
    file_names: List = None,
    path: str = "",
    sep: str = "\t",
) -> None:
    """Add new column _viadot_downloaded_at_utc into every genesys file.

    Args:
        file_names (List, optional): All file names of downloaded files. Defaults to "\t".
        path (str, optional): Relative path to the file. Defaults to empty string.
        sep (str, optional): Separator in csv file. Defaults to None.
    """
    for file in file_names:
        df = pd.read_csv(os.path.join(path, file), sep=sep)
        df_updated = add_ingestion_metadata_task.run(df)
        df_updated.to_csv(os.path.join(path, file), index=False, sep=sep)


@task(timeout=3600)
def filter_userid(
    file_names: list = None,
    path: str = "",
    sep: str = "\t",
    user_ids: list = None,
    apply_method: bool = False,
) -> None:
    """Filter out the data frame by user ID.

    Args:
        file_names (list, optional): All file names of downloaded files. Defaults to "\t".
        path (str, optional): Relative path to the file. Defaults to empty string.
        user_ids (list, optional): List of all user IDs to select in the data frame. Defaults to None.
        sep (str, optional): Separator in csv file. Defaults to None.
        apply_method (bool, optional): Use this method or avoid its execution. Defaults to False.
    """

    if not file_names or not user_ids:
        apply_method = False

    if apply_method:
        for file in file_names:
            df = pd.read_csv(os.path.join(path, file), sep=sep)

            # first: it gets all the conversations ID where an agent is present.
            conversations_id = np.array([])
            for user in user_ids:
                user_filter = df["userId"] == user
                if any(user_filter):
                    ndf = df[user_filter]
                    conversations_id = np.append(
                        conversations_id, ndf["conversationId"]
                    )
            conversations_id = np.unique(conversations_id)

            # second: filter data frame out by the convesation id
            df2 = pd.DataFrame(columns=df.columns)
            for conversation in conversations_id:
                df_tmp = df[df["conversationId"] == conversation]
                df2 = pd.concat([df2, df_tmp])

            df2.to_csv(os.path.join(path, file), index=False, sep=sep)


class GenesysToADLS(Flow):
    def __init__(
        self,
        name: str,
        view_type: str = None,
        view_type_time_sleep: int = 80,
        post_data_list: List[str] = None,
        end_point: str = "analytics/reporting/exports",
        list_of_userids: list = None,
        start_date: str = None,
        end_date: str = None,
        sep: str = "\t",
        environment: str = None,
        report_url: str = None,
        report_columns: List[str] = None,
        conversationId_list: List[str] = None,
        key_list: List[str] = None,
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
            view_type (str, optional): The type of view export job to be created. Defaults to None.
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
            end_point (str, optional): Final end point for Genesys connection. Defaults to "analytics/reporting/exports".
            list_of_userids (list, optional): List of all user IDs to select in the data frame. Defaults to None.
            start_date (str, optional): Start date of the report. Defaults to None.
            end_date (str, optional): End date of the report. Defaults to None.
            sep (str, optional): Separator in csv file. Defaults to "\t".
            environment (str, optional): Adress of host server. Defaults to None than will be used enviroment
                from credentials.
            report_url (str, optional): The url of report generated in json response. Defaults to None.
            report_columns (List[str], optional): List of exisiting column in report. Defaults to None.
            conversationId_list (List[str], optional): List of conversationId passed as attribute of GET method. Defaults to None.
            key_list (List[str], optional): List of keys needed to specify the columns in the GET request method. Defaults to None.
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
        self.end_point = end_point
        if self.end_point == "analytics/conversations/details/query":
            self.apply_method = True
        else:
            self.apply_method = False
        self.list_of_userids = list_of_userids
        self.environment = environment
        self.report_url = report_url
        self.report_columns = report_columns
        self.conversationId_list = conversationId_list
        self.key_list = key_list
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
            timeout=self.timeout,
            local_file_path=self.local_file_path,
            sep=self.sep,
        )

        file_names = to_csv.bind(
            view_type=self.view_type,
            view_type_time_sleep=self.view_type_time_sleep,
            post_data_list=self.post_data_list,
            end_point=self.end_point,
            start_date=self.start_date,
            end_date=self.end_date,
            environment=self.environment,
            conversationId_list=self.conversationId_list,
            key_list=self.key_list,
            credentials_genesys=self.credentials_genesys,
            flow=self,
        )

        filter_userid.bind(
            file_names,
            path=self.local_file_path,
            sep=self.sep,
            user_ids=self.list_of_userids,
            apply_method=self.apply_method,
            flow=self,
        )

        add_timestamp.bind(
            file_names,
            path=self.local_file_path,
            sep=self.sep,
            flow=self,
        )

        adls_bulk_upload.bind(
            file_names=file_names,
            file_name_relative_path=self.local_file_path,
            adls_file_path=self.adls_file_path,
            adls_sp_credentials_secret=self.adls_sp_credentials_secret,
            timeout=self.timeout,
            flow=self,
        )

        filter_userid.set_upstream(file_names, flow=self)
        add_timestamp.set_upstream(filter_userid, flow=self)
        adls_bulk_upload.set_upstream(add_timestamp, flow=self)
