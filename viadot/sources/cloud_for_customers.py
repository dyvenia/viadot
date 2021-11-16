from .base import Source
import requests
import pandas as pd
from typing import Any, Dict, List
from urllib.parse import urljoin
from ..config import local_config
from ..exceptions import APIError
import numpy as np
from requests.adapters import HTTPAdapter
from requests.exceptions import ConnectionError, HTTPError, ReadTimeout, Timeout
from urllib3.exceptions import ProtocolError
import logging
import re
from requests.packages.urllib3.util.retry import Retry


def map_columns(url: str = None) -> Dict:
    column_mapping = {}
    if url:
        credentials = local_config.get("CLOUD_FOR_CUSTOMERS")
        c4c_credentials = credentials.get("Prod")
        auth = (c4c_credentials["username"], c4c_credentials["password"])
        response = requests.get(url, auth=auth)
        for sentence in response.text.split("/>"):
            result = re.search(r'(?<=Name=")([^"]+).+(sap:label=")([^"]+)+', sentence)
            if result:
                key = result.groups(0)[0]
                val = result.groups(0)[2]
                column_mapping[key] = val
    return column_mapping


def change_to_meta_url(url: str) -> str:
    meta_url = ""
    start = url.split(".svc")[0]
    ending = url.split("/")[-1]
    url_end = ending.split("?")[0]
    meta_url = start + ".svc/$metadata?entityset=" + url_end
    return meta_url


def response_to_entity_list(dirty_json: Dict[str, Any], url: str) -> List:
    metadata_url = change_to_meta_url(url)
    column_maper_dict = map_columns(metadata_url)
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


class CloudForCustomers(Source):
    def __init__(
        self,
        *args,
        report_url: str = None,
        url: str = None,
        endpoint: str = None,
        params: Dict[str, Any] = {},
        env: str = "QA",
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
        c4c_credentials = local_config.get("CLOUD_FOR_CUSTOMERS")
        source_credential = c4c_credentials.get(env)
        self.url = url or source_credential["server"]
        self.report_url = report_url
        self.query_endpoint = endpoint
        self.params = params
        self.params["$format"] = "json"
        if source_credential:
            self.auth = (source_credential["username"], source_credential["password"])
        else:
            self.auth = (None, None)

    def to_records(self) -> List:
        entity_list = []
        first = True
        while True:
            if self.report_url:
                url = self.report_url
                response = self.check_url(url)
            elif self.url:
                if first:
                    url = urljoin(self.url, self.query_endpoint)
                    response = self.check_url(url, params=self.params)
                    first = False
                else:
                    response = self.check_url(url)

            if response.status_code == 200:
                dirty_json = response.json()
                if self.report_url:
                    entity_list = response_to_entity_list(dirty_json, url)
                elif self.url:
                    entity_list_np = np.array(entity_list)
                    entity_list = entity_list_np.flatten()
            url = dirty_json["d"].get("__next")
            if url is None:
                break
        return entity_list

    def check_url(self, url: str, params: Dict[str, Any] = None):
        try:
            session = requests.Session()
            retry_strategy = Retry(
                total=3,
                status_forcelist=[429, 500, 502, 503, 504],
                backoff_factor=1,
            )
            adapter = HTTPAdapter(max_retries=retry_strategy)

            session.mount("http://", adapter)
            session.mount("https://", adapter)
            response = session.get(url, params=params, auth=self.auth)
            response.raise_for_status()

        # TODO: abstract below and put as handle_api_response() into utils.py
        except ReadTimeout as e:
            msg = "The connection was successful, "
            msg += f"however the API call to {url} timed out after .... s "
            msg += "while waiting for the server to return data."
            raise APIError(msg)

        except HTTPError as e:
            raise APIError(
                f"The API call to {url} failed. "
                "Perhaps your account credentials need to be refreshed?",
            ) from e

        except (ConnectionError, Timeout) as e:
            raise APIError(
                f"The API call to {url} failed due to connection issues."
            ) from e
        except ProtocolError as e:
            raise APIError(f"Did not receive any reponse for the API call to {url}.")
        except Exception as e:
            raise APIError("Unknown error.") from e

        return response

    def to_df(self, fields: List[str] = None, if_empty: str = None) -> pd.DataFrame:
        records = self.to_records()
        df = pd.DataFrame(data=records)
        if fields:
            return df[fields]
        return df
