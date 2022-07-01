import re
from copy import deepcopy
from typing import Any, Dict, List
from urllib.parse import urljoin

import pandas as pd
import requests

from ..config import local_config
from ..exceptions import CredentialError
from ..utils import handle_api_response
from .base import Source


class CloudForCustomers(Source):
    DEFAULT_PARAMS = {"$format": "json"}

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
        """Cloud for Customers connector build for fetching Odata source.
        See [pyodata docs](https://pyodata.readthedocs.io/en/latest/index.html) for an explanation
        how Odata works.

        Parameters
        ----------
            report_url (str, optional): The url to the API in case of prepared report. Defaults to None.
            url (str, optional): The url to the API. Defaults to None.
            endpoint (str, optional): The endpoint of the API. Defaults to None.
            params (Dict[str, Any]): The query parameters like filter by creation date time. Defaults to json format.
            env (str, optional): The development environments. Defaults to 'QA'.
            credentials (Dict[str, Any], optional): The credentials are populated with values from config file or this
            parameter. Defaults to None than use credentials from local_config.
        """
        super().__init__(*args, **kwargs)

        try:
            DEFAULT_CREDENTIALS = local_config["CLOUD_FOR_CUSTOMERS"].get(env)
        except KeyError:
            DEFAULT_CREDENTIALS = None
        self.credentials = credentials or DEFAULT_CREDENTIALS or {}

        self.url = url or self.credentials.get("server")
        self.report_url = report_url

        if self.url is None and report_url is None:
            raise CredentialError("One of: ('url', 'report_url') is required.")

        self.is_report = bool(report_url)
        self.query_endpoint = endpoint

        if params:
            params_merged = self.DEFAULT_PARAMS.copy()
            params_merged.update(params)

            self.params = params_merged
        else:
            self.params = self.DEFAULT_PARAMS

        if self.url:
            self.full_url = urljoin(self.url, self.query_endpoint)

        super().__init__(*args, credentials=self.credentials, **kwargs)

    @staticmethod
    def change_to_meta_url(url: str) -> str:
        start = url.split(".svc")[0]
        url_raw = url.split("?")[0]
        end = url_raw.split("/")[-1]
        meta_url = start + ".svc/$metadata?entityset=" + end
        return meta_url

    def _to_records_report(self, url: str) -> List[Dict[str, Any]]:
        """Fetches the data from source with report_url.
        At first enter url is from function parameter. At next is generated automaticaly.
        """
        records = []
        while url:
            response = self.get_response(url, params=self.params)
            response_json = response.json()
            new_records = self.response_to_entity_list(response_json, url)
            records.extend(new_records)

            url = response_json["d"].get("__next")

        return records

    def _to_records_other(self, url: str) -> List[Dict[str, Any]]:
        """Fetches the data from source with url.
        At first enter url is a join of url and endpoint passed into this function.
        At any other entering it bring `__next_url` adress, generated automatically, but without params.
        """
        tmp_full_url = deepcopy(url)
        tmp_params = deepcopy(self.params)
        records = []
        while url:
            response = self.get_response(tmp_full_url, params=tmp_params)
            response_json = response.json()
            if isinstance(response_json["d"], dict):
                # ODATA v2+ API
                new_records = response_json["d"].get("results")
                url = response_json["d"].get("__next", None)
            else:
                # ODATA v1
                new_records = response_json["d"]
                url = response_json.get("__next", None)
            # prevents concatenation of previous url's with params with the same params
            tmp_params = None
            tmp_full_url = url
            records.extend(new_records)
        return records

    def to_records(self) -> List[Dict[str, Any]]:
        """
        Download a list of entities in the records format
        """
        if self.is_report:
            url = self.report_url
            return self._to_records_report(url=url)
        else:
            url = self.full_url
            return self._to_records_other(url=url)

    def response_to_entity_list(self, dirty_json: Dict[str, Any], url: str) -> List:

        """Changing request json response to list.

        Args:
            dirty_json (Dict[str, Any]): json from response.
            url (str): the URL which trying to fetch metadata.

        Returns:
            List: List of dictionaries.
        """

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

        """Fetch metadata from url used to column name map.

        Args:
            url (str, optional): the URL which trying to fetch metadata. Defaults to None.

        Returns:
            Dict[str, str]: Property Name as key mapped to the value of sap label.
        """
        column_mapping = {}
        if url:
            username = self.credentials.get("username")
            pw = self.credentials.get("password")
            response = requests.get(url, auth=(username, pw))
            for sentence in response.text.split("/>"):
                result = re.search(
                    r'(?<=Name=")([^"]+).+(sap:label=")([^"]+)+', sentence
                )
                if result:
                    key = result.groups(0)[0]
                    val = result.groups(0)[2]
                    column_mapping[key] = val
        return column_mapping

    def get_response(
        self, url: str, params: Dict[str, Any] = None, timeout: tuple = (3.05, 60 * 30)
    ) -> requests.models.Response:
        """Handle and raise Python exceptions during request. Using of url and service endpoint needs additional parameters
           stores in params. report_url contain additional params in their structure.
           In report_url scenario it can not contain params parameter.

        Args:
            url (str): the URL which trying to connect.
            params (Dict[str, Any], optional): Additional parameters like filter, used in case of normal url.
            Defaults to None used in case of report_url, which can not contain params.
            timeout (tuple, optional): the request times out. Defaults to (3.05, 60 * 30).

        Returns:
            requests.models.Response
        """
        username = self.credentials.get("username")
        pw = self.credentials.get("password")
        response = handle_api_response(
            url=url,
            params=params,
            auth=(username, pw),
            timeout=timeout,
        )
        return response

    def to_df(
        self,
        fields: List[str] = None,
        if_empty: str = "warn",
        dtype: dict = None,
        **kwargs,
    ) -> pd.DataFrame:
        """Returns records in a pandas DataFrame.
        Args:
            fields (List[str], optional): List of fields to put in DataFrame. Defaults to None.
            dtype (dict, optional): The dtypes to use in the DataFrame. We catch this parameter here since
            pandas doesn't support passing dtypes (eg. as a dict) to the constructor.
            kwargs: The parameters to pass to DataFrame constructor.
        """
        records = self.to_records()
        df = pd.DataFrame(data=records, **kwargs)
        if dtype:
            df = df.astype(dtype)
        if fields:
            return df[fields]
        return df
