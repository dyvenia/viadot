from .base import Source
import requests
import pandas as pd
from typing import Any, Dict, List
from urllib.parse import urljoin
from ..config import local_config

import time
from requests.adapters import HTTPAdapter
from requests.exceptions import ConnectionError, HTTPError, ReadTimeout, Timeout
from urllib3.exceptions import ProtocolError
import logging
import re
from requests.packages.urllib3.util.retry import Retry

url = "https://my341115.crm.ondemand.com/sap/c4c/odata/ana_businessanalytics_analytics.svc/$metadata?entityset=RPZ1F90A299C830A29C6659D3QueryResult"


def column_mapper(url=url):
    credentials = local_config.get("CLOUD_FOR_CUSTOMERS")
    access = credentials.get("Prod")
    auth = (access["username"], access["password"])
    response = requests.get(url, auth=auth)
    column_mapping = {}
    for sentence in response.text.split("/>"):
        result = re.search(r'(?<=Name=")([^"]+).+(sap:label=")([^"]+)+', sentence)
        if result:
            key = result.groups(0)[0]
            val = result.groups(0)[2]
            column_mapping[key] = val
    return column_mapping


def change_to_meta_url(url):
    meta_url = ""
    start = url.split(".svc")[0]
    ending = url.split("/")[-1]
    e = ending.split("?")[0]
    meta_url = start + ".svc/$metadata?entityset=" + e
    return meta_url


def response_to_entity_list(dirty_json, url):
    metadata_url = change_to_meta_url(url)
    column_maper_dict = column_mapper(metadata_url)
    entity_list = []
    for element in dirty_json["d"]["results"]:
        new_entity = {}
        for key, object_of_interest in element.items():
            if (
                key != "__metadata"
                and key != "Photo"
                and key != ""
                and key != "Picture"
            ):
                # skip the column which contain nested structure
                if "{" not in str(object_of_interest):
                    new_key = column_maper_dict.get(key)
                    if new_key:
                        new_entity[new_key] = object_of_interest
                    else:
                        new_entity[key] = object_of_interest
        entity_list.append(new_entity)
    return entity_list


class APIError(Exception):
    pass


class CloudForCustomers(Source):
    def __init__(
        self,
        *args,
        direct_url: str = None,
        url: str = None,
        endpoint: str = None,
        params: Dict[str, Any] = {},
        **kwargs,
    ):
        """
        Fetches data from Cloud for Customer.

        Args:
            direct_url (str, optional): The url to the API in case of prepared report. Defaults to None.
            url (str, optional): The url to the API. Defaults to None.
            endpoint (str, optional): The endpoint of the API. Defaults to None.
            params (Dict[str, Any]): The query parameters like filter by creation date time. Defaults to json format.
        """
        super().__init__(*args, **kwargs)
        credentials = local_config.get("CLOUD_FOR_CUSTOMERS")
        self.url = url or credentials["server"]
        self.query_endpoint = endpoint
        self.params = params
        self.params["$format"] = "json"
        access = {"username": None, "password": None}
        if direct_url:
            if "my336539" in direct_url:
                access = credentials.get("QA")
            elif "my341115" in direct_url:
                access = credentials.get("Prod")
        elif url:
            if "my336539" in url:
                access = credentials.get("QA")
            elif "my341115" in url:
                access = credentials.get("Prod")
        self.auth = (access["username"], access["password"])
        self.direct_url = direct_url

    def to_records(self) -> List:
        entity_list = []
        first = True
        while True:
            if self.direct_url:
                url = self.direct_url
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
                entity_from_response = response_to_entity_list(dirty_json, url)
                entity_list.append(entity_from_response)
            url = dirty_json["d"].get("__next")
            if url is None:
                break
        flat_entity_list = [item for sublist in entity_list for item in sublist]
        return flat_entity_list

    def check_url(self, url, params=None):
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
