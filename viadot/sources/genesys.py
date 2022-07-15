import base64
import json
import requests
import sys
import prefect
import pandas as pd
import warnings
from typing import Any, Dict, List
from viadot.config import local_config
from viadot.sources.base import Source
from ..exceptions import CredentialError
from ..utils import handle_api_response

warnings.simplefilter("ignore")


class Genesys(Source):
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

        super().__init__(*args, credentials=self.credentials, **kwargs)

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
                self.logger.info(f"Successfully imported schedule id - {SCHEDULE_ID}.")
            else:
                self.logger.info(f"Please provide schedule id.")

        if self.environment is None:
            self.environment = self.credentials.get("ENVIRONMENT", None)

    @property
    def authorization_token(self):
        """
        Get authorization token with request headers.

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

    @property
    def get_analitics_url_report(self):
        response = handle_api_response(
            url=f"https://api.{self.environment}/api/v2/analytics/reporting/schedules/{self.schedule_id}",
            headers=self.authorization_token,
        )
        response_json = response.json()
        report_url = response_json.get("lastRun", None).get("reportUrl", None)
        self.logger.info("Successfully downloaded report from genesys api")
        return report_url

    def schedule_report(self, data_to_post: Dict[str, Any]):
        payload = json.dumps(data_to_post)
        new_report = handle_api_response(
            url=f"https://api.{self.environment}/api/v2/analytics/reporting/schedules",
            headers=self.authorization_token,
            method="POST",
            body=payload,
        )
        self.logger.info("Succesfully scheduled new report.")
        return True

    def download_report(
        self,
        report_url: str,
        output_file_name: str = None,
        file_extension: str = "xls",
        path: str = "",
    ):
        response_file = handle_api_response(
            url=f"{report_url}", headers=self.authorization_token
        )
        if output_file_name is None:
            final_file_name = f"Genesys_Queue_Metrics_Interval_Export.{file_extension}"
        else:
            final_file_name = f"{output_file_name}.{file_extension}"

        open(f"{path}{final_file_name}", "wb").write(response_file.content)

    def to_df(self, report_url: str = None):
        """
        Most important, how to take report number
        """
        if report_url is None:
            report_url = self.get_analitics_url_report
        response_file = handle_api_response(
            url=f"{report_url}", headers=self.authorization_token
        )

        df = pd.read_excel(response_file.content, names=self.report_columns, skiprows=7)
        self.logger.info("Successfully downloaded report from genesys api")
        return df

    def delete_report(self, report_id: str):
        delete = handle_api_response(
            url=f"https://api.{self.environment}/api/v2/analytics/reporting/schedules/{report_id}",
            headers=self.authorization_token,
            method="DELETE",
        )
        if delete.status_code == 200:
            self.logger.info("Successfully deleted report from genesys api")
        else:
            self.logger.info("Failed to deleted report from genesys api")
