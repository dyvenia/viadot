import os
import json, sys
import base64
import warnings
import asyncio
from typing import Any, Dict, List, Literal
from io import StringIO

import prefect
import aiohttp
import pandas as pd
from datetime import datetime, timedelta
from aiolimiter import AsyncLimiter
from prefect.engine import signals

from viadot.config import local_config
from viadot.sources.base import Source
from viadot.exceptions import CredentialError, APIError
from viadot.utils import handle_api_response


warnings.simplefilter("ignore")


class Genesys(Source):
    def __init__(
        self,
        view_type: str = "queue_performance_detail_view",
        ids_mapping: Dict[str, Any] = None,
        start_date: str = None,
        end_date: str = None,
        report_name: str = None,
        file_extension: Literal["xls", "xlsx", "csv"] = "csv",
        credentials_genesys: Dict[str, Any] = None,
        environment: str = None,
        report_url: str = None,
        schedule_id: str = None,
        report_columns: List[str] = None,
        *args: List[Any],
        **kwargs: Dict[str, Any],
    ):
        """
        Genesys connector which allows for reports scheduling, listing and downloading into Data Frame or specified format output.

        Args:
            view_type (str, optional): The type of view export job to be created. Defaults to "queue_performance_detail_view".
            ids_mapping (str, optional): Dictionary mapping for converting IDs to strings. Defaults to None.
            start_date (str, optional):  Start date of the report. Defaults to None.
            end_date (str, optional):  End date of the report. Defaults to None.
            report_name (str, optional): Name of the report. Defaults to None.
            file_extension (Literal[xls, xlsx, csv;], optional): file extensions for downloaded files. Defaults to "csv".
            credentials_genesys (Dict[str, Any], optional): Credentials to connect with Genesys API containing CLIENT_ID,
            environment (str, optional): Adress of host server. Defaults to None than will be used enviroment
            from credentials.
            report_url (str, optional): The url of report generated in json response. Defaults to None.
            schedule_id (str, optional): The ID of report. Defaults to None.
            report_columns (List[str], optional): List of exisiting column in report. Defaults to None.

        Raises:
            CredentialError: If credentials are not provided in local_config or directly as a parameter.
        """

        self.logger = prefect.context.get("logger")

        if credentials_genesys is not None:
            self.credentials_genesys = credentials_genesys
            self.logger.info("Credentials provided by user")
        else:
            try:
                self.credentials_genesys = local_config["GENESYS"]
                self.logger.info("Credentials loaded from local config")
            except KeyError:
                self.credentials_genesys = None

        if self.credentials_genesys is None:
            raise CredentialError("Credentials not found.")

        super().__init__(*args, credentials=self.credentials_genesys, **kwargs)

        self.view_type = view_type
        self.schedule_id = schedule_id
        self.report_name = report_name
        self.environment = environment
        self.report_url = report_url
        self.report_columns = report_columns

        self.start_date = start_date
        self.end_date = end_date
        self.file_extension = file_extension
        self.ids_mapping = ids_mapping
        self._internal_counter = iter(range(999999))

        if self.schedule_id is None:
            self.schedule_id = self.credentials.get("SCHEDULE_ID", None)

        if self.environment is None:
            self.environment = self.credentials.get("ENVIRONMENT", None)

        if self.ids_mapping is None:
            self.ids_mapping = self.credentials.get("IDS_MAPPING", None)

            if type(self.ids_mapping) is dict and self.ids_mapping is not None:
                self.logger.info("IDS_MAPPING loaded from local credential.")
            else:
                self.logger.warning(
                    "IDS_MAPPING is not provided in you credentials or is not a dictionary."
                )

        self.report_data = []

    @property
    def authorization_token(self, verbose: bool = False):
        """
        Get authorization token with request headers.

        Args:
            CLIENT_SECRET, SCHEDULE_ID and host server. Defaults to None.
            verbose (bool, optional): Switch on/off for logging messages. Defaults to False.

        Returns:
            Dict: request headers with token.
        """
        CLIENT_ID = self.credentials.get("CLIENT_ID", None)
        CLIENT_SECRET = self.credentials_genesys.get("CLIENT_SECRET", None)
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
        if verbose:
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

    def genesys_generate_exports(self, post_data_list: List[str]):
        """
        Function that make POST request method to generate export reports.
        """

        limiter = AsyncLimiter(2, 15)
        semaphore = asyncio.Semaphore(value=1)

        async def generate_post():
            cnt = 0

            for data_to_post in post_data_list:
                if cnt < 10:
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

    def load_reporting_exports(self, page_size: int = 100, verbose: bool = False):
        """
        GET method for reporting export.

        Args:
            page_size (int, optional): The number of items on page to print.
            data_to_post (Dict[str, Any]): json format of POST body.
            verbose (bool, optional): Switch on/off for logging messages. Defaults to False.

        Returns:
            Dict[str, Any]: schedule genesys report.
        """
        new_report = handle_api_response(
            url=f"https://api.{self.environment}/api/v2/analytics/reporting/exports?pageSize={page_size}",
            headers=self.authorization_token,
            method="GET",
        )

        if new_report.status_code == 200:
            if verbose:
                self.logger.info("Succesfully loaded all exports.")
            return new_report.json()
        else:
            self.logger.error(f"Failed to loaded all exports. - {new_report.content}")
            raise APIError("Failed to loaded all exports.")

    def get_reporting_exports_data(self):
        """
        Function that generate list of reports metadata for further processing steps.
        """
        request_json = self.load_reporting_exports()

        if request_json is not None:
            entities = request_json.get("entities")
            assert type(entities) == list
            if len(entities) != 0:
                for entity in entities:
                    tmp = [
                        entity.get("id"),
                        entity.get("downloadUrl"),
                        entity.get("filter").get("queueIds", [-1])[0],
                        entity.get("filter").get("mediaTypes", [-1])[0],
                        entity.get("viewType"),
                        entity.get("interval"),
                        entity.get("status"),
                    ]
                    self.report_data.append(tmp)
            assert len(self.report_data) > 0
        self.logger.info("Generated list of reports entities.")

    def download_report(
        self,
        report_url: str,
        output_file_name: str = None,
        file_extension: str = "csv",
        path: str = "",
        sep: str = "\t",
        drop_duplicates: bool = True,
    ):
        """
        Download report to excel file.

        Args:
            report_url (str): url to report, fetched from json response.
            output_file_name (str, optional): Output file name. Defaults to None.
            file_extension (str, optional): Output file extension. Defaults to "xls".
            path (str, optional): Path to the generated excel file. Defaults to empty string.
            sep (str, optional): Separator in csv file. Defaults to "\t".
            drop_duplicates (bool, optional): Decide if drop duplicates. Defaults to True.
        """
        response_file = handle_api_response(
            url=f"{report_url}", headers=self.authorization_token
        )
        if output_file_name is None:
            final_file_name = f"Genesys_Queue_Metrics_Interval_Export.{file_extension}"
        else:
            final_file_name = f"{output_file_name}.{file_extension}"

        response_data = response_file.content
        df = pd.read_csv(StringIO(response_file.content.decode("utf-8")))
        if drop_duplicates is True:
            df.drop_duplicates(inplace=True, ignore_index=True)
        df.to_csv(os.path.join(path, final_file_name), index=False, sep=sep)

    def download_all_reporting_exports(
        self, store_file_names: bool = True, path: str = ""
    ) -> List[str]:
        """
        Get information form data report and download all files.

        Args:
            ids_mapping (Dict[str, Any], optional): relationship between id and file name. Defaults to None.
            file_extension (Literal[xls, xlsx, csv;], optional): file extensions for downloaded files. Defaults to "csv".
            store_file_names (bool, optional): decide whether to store list of names.
        Returns:
            List[str]: all file names of downloaded files
        """
        file_name_list = []
        temp_ids_mapping = self.ids_mapping
        if temp_ids_mapping is None:
            self.logger.warning("IDS_MAPPING is not provided in you credentials.")
        else:
            self.logger.info("IDS_MAPPING loaded from local credential.")

        for single_report in self.report_data:
            self.logger.info(single_report)
            if single_report[-1] == "RUNNING":
                self.logger.warning(
                    "The request is still in progress and will be deleted, consider add more seconds in `view_type_time_sleep` parameter."
                )
                continue
            elif single_report[-1] == "FAILED":
                self.logger.warning(
                    "This message 'FAILED_GETTING_DATA_FROM_SERVICE' raised during script execution."
                )
                continue
            elif self.start_date not in single_report[5]:
                self.logger.warning(
                    f"The report with ID {single_report[0]} doesn't match with the interval date that you have already defined. \
                        The report won't be downloaded but will be deleted."
                )
                continue

            if single_report[4].lower() == "queue_performance_detail_view":
                file_name = (
                    temp_ids_mapping.get(single_report[2]) + "_" + single_report[3]
                ).upper()
            elif single_report[4].lower() in [
                "agent_performance_summary_view",
                "agent_status_summary_view",
            ]:
                date = self.start_date.replace("-", "")
                file_name = self.view_type.upper() + "_" + f"{date}"
            elif single_report[4].lower() in [
                "agent_status_detail_view",
            ]:
                date = self.start_date.replace("-", "")
                file_name = (
                    self.view_type.upper() + f"_{next(self._internal_counter)}_{date}"
                )
            else:
                raise signals.SKIP(
                    message=f"View type {self.view_type} not defined in viadot, yet..."
                )

            self.download_report(
                report_url=single_report[1],
                path=path,
                output_file_name=file_name,
                file_extension=self.file_extension,
            )
            if store_file_names is True:
                file_name_list.append(file_name + "." + self.file_extension)

        self.logger.info("Al reports were successfully dowonload.")

        if store_file_names is True:
            self.logger.info("Successfully genetared file names list.")
            return file_name_list

    def generate_reporting_export(
        self, data_to_post: Dict[str, Any], verbose: bool = False
    ) -> int:
        """
        POST method for reporting export.

        Args:
            data_to_post (Dict[str, Any]): json format of POST body.
            verbose (bool, optional): Decide if enable logging.

        Returns:
            new_report.status_code: status code
        """
        payload = json.dumps(data_to_post)
        new_report = handle_api_response(
            url=f"https://api.{self.environment}/api/v2/analytics/reporting/exports",
            headers=self.authorization_token,
            method="POST",
            body=payload,
        )
        if verbose:
            if new_report.status_code == 200:
                self.logger.info("Succesfully generated new export.")
            else:
                self.logger.error(
                    f"Failed to generated new export. - {new_report.content}"
                )
                raise APIError("Failed to generated new export.")
        return new_report.status_code

    def delete_reporting_exports(self, report_id):
        """DELETE method for deleting particular reporting exports.

        Args:
            report_id (str): defined at the end of report url.

        Returns:
            delete_method.status_code: status code
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
        """
        Function that deletes all reporting from self.reporting_data list.

        Returns:
            delete_method.status_code: status code
        """
        for report in self.report_data:
            status_code = self.delete_reporting_exports(report_id=report[0])
            assert status_code < 300

        self.logger.info("Successfully removed all reports.")

    # *****************************************************
    #                 WE DON'T USE IT
    # *****************************************************

    def get_analitics_url_report(self):
        """
        Fetching analytics report url from json response.

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

    def get_all_schedules_job(self) -> json:
        """
        Fetching analytics report url from json response.

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

    def schedule_report(self, data_to_post: Dict[str, Any]) -> json:
        """
        POST method for report scheduling.

        Args:
            data_to_post (Dict[str, Any]): json format of POST body.

        Returns:
            new_report.status_code: status code
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

        Returns:
            delete_method.status_code: status code
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
