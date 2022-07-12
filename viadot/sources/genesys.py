import json
import base64, requests, sys, os
from typing import Any, Dict, List
from ..config import local_config
from .base import Source
from ..exceptions import CredentialError
import pandas as pd


class Genesys(Source):
    def __init__(
        self,
        report_name: str,
        queues: List[str] = None,
        credentials: Dict[str, Any] = None,
        enviroment: str = "mypurecloud.de",
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

        authorization = base64.b64encode(
            bytes(
                self.credentials.get("CLIENT_ID")
                + ":"
                + self.credentials.get("CLIENT_SECRET"),
                "ISO-8859-1",
            )
        ).decode("ascii")
        request_headers = {
            "Authorization": f"Basic {authorization}",
            "Content-Type": "application/x-www-form-urlencoded",
        }
        response = requests.post(
            f"https://login.{enviroment}/oauth/token",
            data=request_body,
            headers=request_headers,
        )
        if response.status_code == 200:
            print("Got token")
        else:
            print(f"Failure: { str(response.status_code) } - { response.reason }")
            sys.exit(response.status_code)
        response_json = response.json()

        self.requestHeaders = {
            "Authorization": f"{ response_json['token_type'] } { response_json['access_token']}",
            "Content-Type": "application/json",
        }

        self.report_name = report_name
        self.queues = queues
        self.enviroment = enviroment

    def get_analitics(self):
        response = requests.get(
            f"https://api.{self.enviroment}/api/v2/analytics/reporting/schedules/",
            headers=self.requestHeaders,
        )
        return response

    def schedule_report(self, data_to_post: Dict[str, Any]):
        payload = json.dumps(data_to_post)
        new_report = requests.post(
            f"https://api.{self.enviroment}/api/v2/analytics/reporting/schedules",
            headers=self.requestHeaders,
            data=payload,
        )

    def fetch_report_to_xlsx(self, report_number: str, columns: List[str]):
        """
        Most important, how to take report number
        """
        response_file = requests.get(
            f"https://apps.{self.enviroment}/platform/api/v2/downloads/{report_number}",
            headers=self.requestHeaders,
        )
        open("raport_file.xls", "wb").write(response_file.content)
        df = pd.read_xls("raport_file.xls", columns=columns, skiprows=7)
        os.remove("raport_file.xls")

        return df

    def delete_report(self, report_id: str):
        delete = requests.delete(
            f"https://api.{self.enviroment}/api/v2/analytics/reporting/schedules/{report_id}",
            headers=self.requestHeaders,
        )
