from typing import Any, Dict, List, Tuple, Literal
import pandas as pd
from prefect import Task, task
from prefect.utilities import logging
from prefect.utilities.tasks import defaults_from_attrs
import prefect
from ..exceptions import CredentialError
from datetime import datetime, timedelta
from viadot.config import local_config
import asyncio
import aiohttp
import time
import json
from aiolimiter import AsyncLimiter
import pandas as pd
import numpy as np
from viadot.tasks import AzureDataLakeUpload


from ..sources import Genesys

logger = logging.get_logger()


IDS_MAPPING = {
    "V_D_PROD_FB_Queue": "780807e6-83b9-44be-aff0-a41c37fab004",
    "V_E_PROD_Voice_Dealer_Queue": "383ee5e5-5d8f-406c-ad53-835b69fe82c5",
    "V_E_PROD_Voice_Enduser_Queue": "d4fa7080-63d3-47ea-bcde-a4f89ae2870c",
    "V_E_PROD_Voice_Installer_Queue": "427b6226-cafd-4425-ba20-4e7dd8561088",
    "V_E_PROD_Voice_Logistics_Queue": "e0efef7f-b61a-4caf-b71b-002595c6899e",
    "V_P_PROD_Voice_Dealer_Queue": "4c757061-b2cd-419e-bae3-4700730df4a6",
    "V_P_PROD_Voice_Enduser_Queue": "b44af752-2ce4-4810-8b83-6d131f493e23",
    "V_P_PROD_Voice_Installer_Queue": "247586a2-986f-4078-b28e-e0a5694ec726",
}


class GenesysToDF(Task):
    def __init__(
        self,
        report_name: str = None,
        credentials: Dict[str, Any] = None,
        environment: str = None,
        schedule_id: str = None,
        report_url: str = None,
        report_columns: List[str] = None,
        *args: List[Any],
        **kwargs: Dict[str, Any],
    ):

        try:
            DEFAULT_CREDENTIALS = local_config["GENESYS"]
        except KeyError:
            DEFAULT_CREDENTIALS = None
        self.credentials = credentials or DEFAULT_CREDENTIALS
        if self.credentials is None:
            raise CredentialError("Credentials not found.")

        self.logger = prefect.context.get("logger")
        self.schedule_id = schedule_id
        self.report_name = report_name
        self.environment = environment
        self.report_url = report_url
        self.report_columns = report_columns

        # Get schedule id to retrive report url
        if self.schedule_id is None:
            SCHEDULE_ID = self.credentials.get("SCHEDULE_ID", None)
            if SCHEDULE_ID is not None:
                self.schedule_id = SCHEDULE_ID

        if self.environment is None:
            self.environment = self.credentials.get("ENVIRONMENT", None)

        super().__init__(
            name="genesys_to_df",
            *args,
            **kwargs,
        )

    def __call__(self, *args, **kwargs):
        """Download Genesys data to DF"""
        return super().__call__(*args, **kwargs)

    @defaults_from_attrs(
        "report_name", "environment", "schedule_id", "report_url", "report_columns"
    )
    def run(
        self,
        report_name: str = None,
        environment: str = None,
        schedule_id: str = None,
        report_url: str = None,
        report_columns: List[str] = None,
    ) -> pd.DataFrame:
        """
        Task for downloading data from the Genesys API to DF.

        Args:
            report_name (str, optional): Name of the report. Defaults to None.
            environment (str, optional): Adress of host server. Defaults to None than will be used enviroment
            from credentials.
            schedule_id (str, optional): The ID of report. Defaults to None.
            report_url (str, optional): The url of report generated in json response. Defaults to None.
            report_columns (List[str], optional): List of exisiting column in report. Defaults to None.

        Returns:
            pd.DataFrame: The API GET as a pandas DataFrames from Genesys.
        """
        genesys = Genesys(
            report_name=report_name,
            credentials=self.credentials,
            environment=environment,
            schedule_id=schedule_id,
            report_url=report_url,
            report_columns=report_columns,
        )

        df = genesys.to_df()

        logger.info(f"Downloaded the data from the Genesys into the Data Frame.")
        return df


class GenesysExportsToCSV(Task):
    def __init__(
        self,
        queueIds_list: List[str] = None,
        media_type_list: List[str] = None,
        credentials: Dict[str, Any] = None,
        environment: str = None,
        *args: List[Any],
        **kwargs: Dict[str, Any],
    ):

        try:
            DEFAULT_CREDENTIALS = local_config["GENESYS"]
        except KeyError:
            DEFAULT_CREDENTIALS = None
        self.credentials = credentials or DEFAULT_CREDENTIALS
        if self.credentials is None:
            raise CredentialError("Credentials not found.")

        self.logger = prefect.context.get("logger")
        self.queueIds_list = queueIds_list
        self.media_type_list = media_type_list
        self.environment = environment

        super().__init__(
            name="genesys_exports_to_df",
            *args,
            **kwargs,
        )

    @defaults_from_attrs(
        "report_name", "environment", "schedule_id", "report_url", "report_columns"
    )
    def run(self):
        return True


@task()
def genesys_generate_body(
    days_interval=1,
    start_date=None,
    end_date=None,
    media_type_list=None,
    queueIds_list=None,
    data_to_post_str=None,
):
    """data_to_post

    example string:
        mystr = '''{
            "name": f"QUEUE_PERFORMANCE_DETAIL_VIEW_{media}",
            "timeZone": "UTC",
            "exportFormat": "CSV",
            "interval": f"{end_date}T23:00:00/{start_date}T23:00:00",
            "period": "PT30M",
            "viewType": f"QUEUE_PERFORMANCE_DETAIL_VIEW",
            "filter": {"mediaTypes": [f"{media}"], "queueIds": [f"{queueid}"], "directions":["inbound"],},
            "read": True,
            "locale": "en-us",
            "hasFormatDurations": True,
            "hasSplitFilters": True,
            "excludeEmptyRows": True,
            "hasSplitByMedia": True,
            "hasSummaryRow": True,
            "csvDelimiter": "COMMA",
            "hasCustomParticipantAttributes": True,
            "recipientEmails": [],
            }'''
    """
    post_data_list = []

    if start_date is None and end_date is None:
        today = datetime.now()
        yesterday = today - timedelta(days=days_interval)
        start_date = today.strftime("%Y-%m-%d")
        end_date = yesterday.strftime("%Y-%m-%d")
    else:
        end_date = datetime.strptime(end_date, "%Y-%m-%d")

        start_date = datetime.strptime(start_date, "%Y-%m-%d")

    for media in media_type_list:
        for queueid in queueIds_list:
            data_to_post = eval(data_to_post_str)

            post_data_list.append(data_to_post)

    return post_data_list


@task
def genesys_generate_exports(authorization_token=None, post_data_list=None):
    """call a source"""

    limiter = AsyncLimiter(2, 15)
    semaphore = asyncio.Semaphore(value=1)

    async def generate_post(post_data_list):
        cnt = 0
        for data_to_post in post_data_list:
            if cnt != 10:
                payload = json.dumps(data_to_post)
                async with aiohttp.ClientSession() as session:
                    await semaphore.acquire()
                    async with limiter:
                        async with session.post(
                            "https://api.mypurecloud.de/api/v2/analytics/reporting/exports",
                            headers=authorization_token,
                            data=payload,
                        ) as resp:
                            new_report = await resp.read()
                            logger.info(f"--- {cnt} ---  {new_report}.")
                            semaphore.release()
                cnt += 1
            else:
                await asyncio.sleep(3)
                cnt = 0

    loop = asyncio.get_event_loop()
    coroutine = generate_post(post_data_list)
    loop.run_until_complete(coroutine)


@task
def get_reporting_exports_data(request_json: Dict[str, Any] = None):
    exports_data = []
    if request_json is not None:
        entities = request_json.get("entities")
        if len(entities) != 0:
            for e in entities:
                tmp = [
                    e.get("id"),
                    e.get("downloadUrl"),
                    e.get("filter").get("queueIds")[0],
                    e.get("filter").get("mediaTypes")[0],
                ]
                exports_data.append(tmp)
            return exports_data


@task
def download_all_reporting_exports(
    ids_mapping: Dict[str, Any] = None,
    report_data: List[str] = None,
    file_extension: Literal["xls", "xlsx", "csv"] = "csv",
    g_instance=None,
    store_file_names: bool = True,
) -> List[str]:
    """Get information form data report and download all files.
    Args:
        g_instance (Genesys): instance of Genesys source
        ids_mapping (Dict[str, Any], optional): relationship between id and file name. Defaults to None.
        data_report (List[str], optional): data extracted from genesys. Defaults to None.
        file_extension (Literal[xls, xlsx, csv;], optional): file extensions for downloaded files. Defaults to "csv".
        store_file_names (bool, optional): decide whether to store list of names.
    Returns:
        List[str]: all file names of downloaded files
    """
    file_name_list = []
    for single_report in report_data:
        file_name = (
            ids_mapping.get(single_report[2]) + "_" + single_report[-1]
        ).upper()
        g_instance.download_report(
            report_url=single_report[1],
            output_file_name=file_name,
            file_extension=file_extension,
        )
        if store_file_names is True:
            file_name_list.append(file_name + "." + file_extension)

    if store_file_names is True:
        return file_name_list


@task
def genesys_files_uploader(
    file_names_list: List[str] = None,
    adls_path: str = None,
    overwrite: bool = True,
    sp_credentials_secret: str = None,
):

    upload_task = AzureDataLakeUpload()

    for file in file_names_list:
        # ADL Class

        print(adls_path + "/" + f"{file}")
        # upload data to ADLS
        upload_task.run(
            from_path=file,
            to_path=adls_path + f"{file}",
            overwrite=overwrite,
            sp_credentials_secret=sp_credentials_secret,
        )


@task
def delete_all_reports(reporting_data: List[str], genesys_instance: str = None):

    ids_list = list(np.array(reporting_data).T[0])
    try:
        for id in ids_list:
            genesys_instance.delete_reporting_exports(id)
    except Exception as e:
        logger.error("Output data error: " + str(type(e).__name__) + ": " + str(e))
