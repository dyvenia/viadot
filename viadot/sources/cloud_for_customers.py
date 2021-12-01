from .base import Source
import requests
import pandas as pd
from typing import Any, Dict, List
from urllib.parse import urljoin
from ..config import local_config
from ..utils import handle_api_response
import re


class CloudForCustomers(Source):
    def __init__(
        self,
        *args,
        report_url: str = None,
        url: str = None,
        endpoint: str = None,
        params: Dict[str, Any] = None,
        env: str = "QA",
        credentials: Dict[str, Any] = None,
        **kwargs,
    ):
        """
        Fetches data from Cloud for Customer.

        Args:
            report_url (str, optional): The url to the API in case of prepared report. Defaults to None.
            url (str, optional): The url to the API. Defaults to None.
            endpoint (str, optional): The endpoint of the API. Defaults to None.
            params (Dict[str, Any]): The query parameters like filter by creation date time. Defaults to json format.
            env (str, optional): The development environments. Defaults to 'QA'.
        """
        super().__init__(*args, **kwargs)

        try:
            DEFAULT_CREDENTIALS = local_config["CLOUD_FOR_CUSTOMERS"].get(env)
        except KeyError:
            DEFAULT_CREDENTIALS = None
        credentials = credentials or DEFAULT_CREDENTIALS or {}

        self.url = url or credentials.get("server")
        self.report_url = report_url

        if self.url is None and report_url is None:
            raise CredentialError("One of: ('url', 'report_url') is required.")

        self.is_report = bool(report_url)
        self.query_endpoint = endpoint
        self.params = params or {}
        self.params["$format"] = "json"
        if credentials:
            self.auth = (credentials["username"], credentials["password"])
        else:
            self.auth = (None, None)

        if self.url:
            self.full_url = urljoin(self.url, self.query_endpoint)

        super().__init__(*args, credentials=credentials, **kwargs)

    @staticmethod
    def change_to_meta_url(url: str) -> str:
        start = url.split(".svc")[0]
        url_raw = url.split("?")[0]
        end = url_raw.split("/")[-1]
        meta_url = start + ".svc/$metadata?entityset=" + end
        return meta_url

    def _to_records_report(self, url: str) -> List[Dict[str, Any]]:
        records = []
        while url:
            response = self.get_response(url)
            response_json = response.json()
            new_records = self.response_to_entity_list(response_json, url)
            records.extend(new_records)

            url = response_json["d"].get("__next")

        return records

    def _to_records_other(self, url: str) -> List[Dict[str, Any]]:
        records = []
        while url:
            response = self.get_response(self.full_url, params=self.params)
            response_json = response.json()
            if isinstance(response_json["d"], dict):
                # ODATA v2+ API
                new_records = response_json["d"].get("results")
                url = response_json["d"].get("__next")
            else:
                # ODATA v1
                new_records = response_json["d"]
                url = response_json.get("__next")

            records.extend(new_records)

        return records

    def to_records(self) -> List[Dict[str, Any]]:
        """Download a list of entities in the records format"""
        if self.is_report:
            url = self.report_url
            return self._to_records_report(url=url)
        else:
            url = self.full_url
            return self._to_records_other(url=url)

    def response_to_entity_list(self, dirty_json: Dict[str, Any], url: str) -> List:
        metadata_url = self.change_to_meta_url(url)
        column_maper_dict = self.map_columns(metadata_url)
        entity_list = []
        for element in dirty_json["d"]["results"]:
            new_entity = {}
            for key, object_of_interest in element.items():
                if key not in ["__metadata", "Photo", "", "Picture"]:
                    if "{" not in str(object_of_interest):
                        new_key = column_maper_dict.get(key)
                        if new_key:
                            new_entity[new_key] = object_of_interest
                        else:
                            new_entity[key] = object_of_interest
            entity_list.append(new_entity)
        return entity_list

    def map_columns(self, url: str = None) -> Dict[str, str]:
        column_mapping = {}
        if url:
            auth = (self.credentials["username"], self.credentials["password"])
            response = requests.get(url, auth=auth)
            for sentence in response.text.split("/>"):
                result = re.search(
                    r'(?<=Name=")([^"]+).+(sap:label=")([^"]+)+', sentence
                )
                if result:
                    key = result.groups(0)[0]
                    val = result.groups(0)[2]
                    column_mapping[key] = val
        return column_mapping

    def get_response(self, url: str, timeout: tuple = (3.05, 60 * 30)):
        response = handle_api_response(
            url=url, params=self.params, auth=self.auth, timeout=timeout
        )
        return response

    def to_df(self, fields: List[str] = None, if_empty: str = "warn") -> pd.DataFrame:
        records = self.to_records()
        df = pd.DataFrame(data=records)
        if fields:
            return df[fields]
        return df
