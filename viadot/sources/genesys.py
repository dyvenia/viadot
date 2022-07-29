import json
import base64
import warnings
from typing import Any, Dict, List, Literal

import prefect
import asyncio
import aiohttp
import pandas as pd
from datetime import datetime, timedelta
from aiolimiter import AsyncLimiter

from viadot.config import local_config
from viadot.sources.base import Source
from ..exceptions import CredentialError, APIError
from ..utils import handle_api_response


warnings.simplefilter("ignore")


class Genesys(Source):
    def __init__(
        self,
        media_type_list: List[str] = None,
        queueIds_list: List[str] = None,
        data_to_post_str: str = None,
        ids_mapping: Dict[str, Any] = None,
        start_date: str = None,
        end_date: str = None,
        days_interval: int = 1,
        report_name: str = None,
        file_extension: Literal["xls", "xlsx", "csv"] = "csv",
        credentials: Dict[str, Any] = None,
        environment: str = None,
        report_url: str = None,
        schedule_id: str = None,
        report_columns: List[str] = None,
        *args: List[Any],
        **kwargs: Dict[str, Any],
    ):
        """
        Genesys connector which allow for reports scheduling, listing and downloading into Data Frame or Specified fromat output.

        Args:
            media_type_list (List[str], optional):  List of specific media types. Defaults to None.
            queueIds_list (List[str], optional):  List of specific queues ids. Defaults to None.
            data_to_post_str (str, optional):  String template to generate json body. Defaults to None.
            ids_mapping (str, optional): Dictionary mapping for converting IDs to strings. Defaults to None.
            start_date (str, optional):  Start date of the report. Defaults to None.
            end_date (str, optional):  End date of the report. Defaults to None.
            days_interval (int, optional):  How many days report should include. Defaults to None.
            report_name (str, optional): Name of the report. Defaults to None.
            file_extension (Literal[xls, xlsx, csv;], optional): file extensions for downloaded files. Defaults to "csv".
            credentials (Dict[str, Any], optional): Credentials to connect with Genesys API containing CLIENT_ID,
            environment (str, optional): Adress of host server. Defaults to None than will be used enviroment
            from credentials.
            report_url (str, optional): The url of report generated in json response. Defaults to None.
            schedule_id (str, optional): The ID of report. Defaults to None.
            report_columns (List[str], optional): List of exisiting column in report. Defaults to None.

        Raises:
            CredentialError: If credentials are not provided in local_config or directly as a parameter.
        """

        try:
            DEFAULT_CREDENTIALS = local_config["GENESYS"]
        except KeyError:
            DEFAULT_CREDENTIALS = None
        self.credentials = credentials or DEFAULT_CREDENTIALS
        if self.credentials is None:
            raise CredentialError("Credentials not found.")

        super().__init__(*args, credentials=self.credentials, **kwargs)

        self.logger = prefect.context.get("logger")
        self.schedule_id = schedule_id
        self.report_name = report_name
        self.environment = environment
        self.report_url = report_url
        self.report_columns = report_columns
        # self.authorization_token = None

        self.start_date = start_date
        self.end_date = end_date
        self.days_interval = days_interval
        self.media_type_list = media_type_list
        self.queueIds_list = queueIds_list
        self.data_to_post_str = data_to_post_str
        self.file_extension = file_extension
        self.ids_mapping = ids_mapping

        # Get schedule id to retrive report url
        if self.schedule_id is None:
            self.schedule_id = self.credentials.get("SCHEDULE_ID", None)
            # if SCHEDULE_ID is not None:
            #     self.schedule_id = SCHEDULE_ID

        if self.environment is None:
            self.environment = self.credentials.get("ENVIRONMENT", None)

        if self.ids_mapping is None:
            self.ids_mapping = self.credentials.get("IDS_MAPPING", None)

        self.post_data_list = []
        self.report_data = []

    @property
    def authorization_token(self):
        """
        Get authorization token with request headers.

        Locals:
            CLIENT_SECRET, SCHEDULE_ID and host server. Defaults to None.

        Returns:
            Dict: request headers with token.
        """
        CLIENT_ID = self.credentials.get("CLIENT_ID", None)
        CLIENT_SECRET = self.credentials.get("CLIENT_SECRET", None)
        authorization = base64.b64encode(
            bytes(CLIENT_ID + ":" + CLIENT_SECRET, "ISO-8859-1")
        ).decode("ascii")
        request_headers = {
            "Authorization": f"Basic {authorization}",
            "Content-Type": "application/x-www-form-urlencoded",
        }
        request_body = {"grant_type": "client_credentials"}
        response = handle_api_response(
            f"https://login.{self.environment}/oauth/token",
            body=request_body,
            headers=request_headers,
            method="POST",
        )
        if response.status_code == 200:
            self.logger.info("Temporary authorization token was generated.")
        else:
            self.logger.info(
                f"Failure: { str(response.status_code) } - { response.reason }"
            )
        response_json = response.json()
        request_headers = {
            "Authorization": f"{ response_json['token_type'] } { response_json['access_token']}",
            "Content-Type": "application/json",
        }

        return request_headers

    def genesys_generate_body(self):
        """
        Function that generate json body for Post request method.

        example string:
            example_raw_json_body_string = '''{
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

        Returns: self.post_data_list (List[str]): List of formated POST body.

        """

        if self.start_date is None and self.end_date is None:
            today = datetime.now()
            yesterday = today - timedelta(days=self.days_interval)
            start_date = today.strftime("%Y-%m-%d")
            end_date = yesterday.strftime("%Y-%m-%d")
        else:
            start_date = datetime.strptime(start_date, "%Y-%m-%d")
            end_date = datetime.strptime(end_date, "%Y-%m-%d")

        for media in self.media_type_list:
            for queueid in self.queueIds_list:
                data_to_post = eval(self.data_to_post_str)

                self.post_data_list.append(data_to_post)

        return self.post_data_list

    def genesys_generate_exports(self):
        """
        Function that make POST request method to generate export reports.
        """

        limiter = AsyncLimiter(2, 15)
        semaphore = asyncio.Semaphore(value=1)

        async def generate_post():
            cnt = 0

            for data_to_post in self.post_data_list:
                print("--" * 20)
                print(data_to_post)
                print("--" * 20)
                if cnt != 10:
                    payload = json.dumps(data_to_post)
                    async with aiohttp.ClientSession() as session:
                        await semaphore.acquire()
                        async with limiter:
                            async with session.post(
                                f"https://api.{self.environment}/api/v2/analytics/reporting/exports",
                                headers=self.authorization_token,
                                data=payload,
                            ) as resp:
                                new_report = await resp.read()
                                self.logger.info(
                                    f"Generated report export --- \n {payload}."
                                )
                                semaphore.release()
                    cnt += 1
                else:
                    await asyncio.sleep(3)
                    cnt = 0

        loop = asyncio.get_event_loop()
        coroutine = generate_post()
        loop.run_until_complete(coroutine)

    def get_reporting_exports_data(self):
        """
        Function that generate list of reports metadata for further processing.


        """
        request_json = self.load_reporting_exports()

        if request_json is not None:
            entities = request_json.get("entities")
            if len(entities) != 0:
                for entity in entities:
                    tmp = [
                        entity.get("id"),
                        entity.get("downloadUrl"),
                        entity.get("filter").get("queueIds")[0],
                        entity.get("filter").get("mediaTypes")[0],
                    ]
                    self.report_data.append(tmp)
        self.logger.info("Generated list of reports entities.")

    def download_all_reporting_exports(
        self,
        store_file_names: bool = True,
    ) -> List[str]:
        """
        Get information form data report and download all files.

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
        for single_report in self.report_data:
            file_name = (
                self.ids_mapping.get(single_report[2]) + "_" + single_report[-1]
            ).upper()
            self.download_report(
                report_url=single_report[1],
                output_file_name=file_name,
                file_extension=self.file_extension,
            )
            if store_file_names is True:
                file_name_list.append(file_name + "." + self.file_extension)

        if store_file_names is True:
            return file_name_list

    def get_analitics_url_report(self):
        """Fetching analytics report url from json response.

        Returns:
            string: url for analytics report
        """
        response = handle_api_response(
            url=f"https://api.{self.environment}/api/v2/analytics/reporting/schedules/{self.schedule_id}",
            headers=self.authorization_token,
        )
        try:
            response_json = response.json()
            report_url = response_json.get("lastRun", None).get("reportUrl", None)
            self.logger.info("Successfully downloaded report from genesys api")
            return report_url
        except AttributeError as e:
            self.logger.error(
                "Output data error: " + str(type(e).__name__) + ": " + str(e)
            )

    def get_all_schedules_job(self):
        """Fetching analytics report url from json response.

        Returns:
            string: json body with all schedules jobs.
        """
        response = handle_api_response(
            url=f"https://api.{self.environment}/api/v2/analytics/reporting/schedules",
            headers=self.authorization_token,
        )
        try:
            response_json = response.json()
            self.logger.info("Successfully downloaded schedules jobs.")
            return response_json
        except AttributeError as e:
            self.logger.error(
                "Output data error: " + str(type(e).__name__) + ": " + str(e)
            )

    def schedule_report(self, data_to_post: Dict[str, Any]):
        """POST method for report scheduling.

        Args:
            data_to_post (Dict[str, Any]): json format of POST body.

        Returns:
            bool: schedule genesys report
        """
        payload = json.dumps(data_to_post)
        new_report = handle_api_response(
            url=f"https://api.{self.environment}/api/v2/analytics/reporting/schedules",
            headers=self.authorization_token,
            method="POST",
            body=payload,
        )
        if new_report.status_code == 200:
            self.logger.info("Succesfully scheduled new report.")
        else:
            self.logger.error(f"Failed to scheduled new report. - {new_report.content}")
            raise APIError("Failed to scheduled new report.")
        return new_report.status_code

    def load_reporting_exports(self, page_size: int = 100):
        """GET method for reporting export.

        Args:
            data_to_post (Dict[str, Any]): json format of POST body.

        Returns:
            bool: schedule genesys report
        """
        new_report = handle_api_response(
            url=f"https://api.{self.environment}/api/v2/analytics/reporting/exports?pageSize={page_size}",
            headers=self.authorization_token,
            method="GET",
        )
        if new_report.status_code == 200:
            self.logger.info("Succesfully loaded all exports.")
            return new_report.json()
        else:
            self.logger.error(f"Failed to loaded all exports. - {new_report.content}")
            raise APIError("Failed to loaded all exports.")

    def generate_reporting_export(self, data_to_post: Dict[str, Any]):
        """POST method for reporting export.

        Args:
            data_to_post (Dict[str, Any]): json format of POST body.

        Returns:
            bool: schedule genesys report
        """
        payload = json.dumps(data_to_post)
        new_report = handle_api_response(
            url=f"https://api.{self.environment}/api/v2/analytics/reporting/exports",
            headers=self.authorization_token,
            method="POST",
            body=payload,
        )
        if new_report.status_code == 200:
            self.logger.info("Succesfully generated new export.")
        else:
            self.logger.error(f"Failed to generated new export. - {new_report.content}")
            raise APIError("Failed to generated new export.")
        return new_report.status_code

    def download_report(
        self,
        report_url: str,
        output_file_name: str = None,
        file_extension: str = "xls",
        path: str = "",
    ):
        """Download report to excel file.

        Args:
            report_url (str): url to report, fetched from json response.
            output_file_name (str, optional): Output file name. Defaults to None.
            file_extension (str, optional): Output file extension. Defaults to "xls".
            path (str, optional): Path to the generated excel file. Defaults to empty string.
        """
        response_file = handle_api_response(
            url=f"{report_url}", headers=self.authorization_token
        )
        if output_file_name is None:
            final_file_name = f"Genesys_Queue_Metrics_Interval_Export.{file_extension}"
        else:
            final_file_name = f"{output_file_name}.{file_extension}"

        with open(f"{path}{final_file_name}", "wb") as file:
            file.write(response_file.content)

    def to_df(self, report_url: str = None):
        """Download genesys data into a pandas DataFrame.

        Args:
            report_url (str): Report url from api response.

        Returns:
            pd.DataFrame: the DataFrame with time range
        """
        if report_url is None:
            report_url = self.get_analitics_url_report
        response_file = handle_api_response(
            url=f"{report_url}", headers=self.authorization_token
        )
        if self.report_columns is None:
            df = pd.read_excel(response_file.content, header=6)
        else:
            df = pd.read_excel(
                response_file.content, names=self.report_columns, skiprows=6
            )

        return df

    def delete_scheduled_report_job(self, report_id: str):
        """DELETE method for deleting particular report job.

        Args:
            report_id (str): defined at the end of report url.
        """
        delete_method = handle_api_response(
            url=f"https://api.{self.environment}/api/v2/analytics/reporting/schedules/{report_id}",
            headers=self.authorization_token,
            method="DELETE",
        )
        if delete_method.status_code == 200:
            self.logger.info("Successfully deleted report from Genesys API.")

        else:
            self.logger.error(
                f"Failed to deleted report from Genesys API. - {delete_method.content}"
            )
            raise APIError("Failed to deleted report from Genesys API.")

        return delete_method.status_code

    def delete_reporting_exports(self, report_id):
        """DELETE method for deleting particular reporting exports.

        Args:
            report_id (str): defined at the end of report url.
        """
        delete_method = handle_api_response(
            url=f"https://api.{self.environment}/api/v2/analytics/reporting/exports/{report_id}",
            headers=self.authorization_token,
            method="DELETE",
        )
        if delete_method.status_code < 300:
            self.logger.info("Successfully deleted report from Genesys API.")

        else:
            self.logger.error(
                f"Failed to deleted report from Genesys API. - {delete_method.content}"
            )
            raise APIError("Failed to deleted report from Genesys API.")

        return delete_method.status_code

    def delete_all_reporting_exports(self):
        """Function that deletes all reporting from self.reporting_data list."""
        for report in self.report_data:
            self.delete_reporting_exports(report_id=report[0])
