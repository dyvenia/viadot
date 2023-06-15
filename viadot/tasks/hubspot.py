import json
from typing import Any, Dict, List, Literal

import pandas as pd
from prefect import Task
from prefect.tasks.secrets import PrefectSecret
from prefect.utilities import logging

from viadot.exceptions import ValidationError
from viadot.sources import Hubspot
from viadot.task_utils import *

logger = logging.get_logger()


class HubspotToDF(Task):
    def __init__(
        self,
        hubspot_credentials: dict = None,
        hubspot_credentials_key: str = "HUBSPOT",
        *args,
        **kwargs,
    ):
        """
        Class for generating dataframe from Hubspot API endpoint.

        Args:
            hubspot_credentials (dict): Credentials to Hubspot API. Defaults to None.
            hubspot_credentials_key (str, optional): Credential key to dictionary where credentials are stored (e.g. in local config). Defaults to "HUBSPOT".

        """

        if hubspot_credentials is None:
            self.hubspot_credentials = credentials_loader.run(
                credentials_secret=hubspot_credentials_key,
            )

        else:
            self.hubspot_credentials = hubspot_credentials

        super().__init__(
            name="hubspot_to_df",
            *args,
            **kwargs,
        )

    def __call__(self):
        """Download Hubspot data to a DF"""
        super().__call__(self)

    def to_df(self, result: dict) -> pd.DataFrame:
        """
        Args:
            result (dict): API response in JSON format.

        Returns:
            pd.DataFrame: Output table generated from JSON dictionary.
        """
        return pd.json_normalize(result)

    def date_to_unixtimestamp(self, date: str = None) -> int:
        """
        Function for date conversion from user defined "yyyy-mm-dd" to Unix Timestamp (SECONDS SINCE JAN 01 1970. (UTC)).
        For example: 1680774921 SECONDS SINCE JAN 01 1970. (UTC) -> 11:55:49 AM 2023-04-06.

        Args:
            date (str, optional): Input date in format "yyyy-mm-dd". Defaults to None.

        Returns:
            int: Number of seconds that passed since 1970-01-01 until "date".
        """

        clean_date = int(datetime.timestamp(datetime.strptime(date, "%Y-%m-%d")) * 1000)
        return clean_date

    def format_filters(self, filters: Dict[str, Any] = {}) -> Dict[str, Any]:
        """
        Function for API body (filters) conversion from a user defined to API language. Converts date to Unix Timestamp.

        Args:
            filters (Dict[str, Any], optional): Filters in JSON format. Defaults to {}.

        Returns:
            Dict[str, Any]: Filters in JSON format after data cleaning.
        """
        for item in filters:
            for iterator in range(len(item["filters"])):
                for subitem in item["filters"][iterator]:
                    lookup = item["filters"][iterator][subitem]
                    try:
                        date_after_format = self.date_to_unixtimestamp(lookup)
                        filters[filters.index(item)]["filters"][iterator][
                            subitem
                        ] = date_after_format
                    except:
                        pass

        return filters

    def get_offset_from_response(self, api_response: Dict[str, Any] = {}) -> tuple:
        """
        Helper funtion for assigning offset type/value depends on keys in API response.

        Args:
            api_response (Dict[str, Any], optional): API response in JSON format. Defaults to {}.

        Returns:
            tuple: Tuple in order: (offset_type, offset_value)
        """
        if "paging" in api_response.keys():
            offset_type = "after"
            offset_value = api_response["paging"]["next"][f"{offset_type}"]
        elif "offset" in api_response.keys():
            offset_type = "offset"
            offset_value = api_response["offset"]
        else:
            offset_type = None
            offset_value = None

        return (offset_type, offset_value)

    def run(
        self,
        endpoint: str,
        properties: List[Any] = [],
        filters: Dict[str, Any] = {},
        nrows: int = 1000,
    ) -> pd.DataFrame:
        """
        Run method for extraction of defined varieties of endpints for Hubspot API.

        Args:
            endpoint (str): Hubspot endpoint - full url or schema name (contacts/line_items/,...).
            properties (List[Any], optional): List of columns for the extraction. Defaults to [].
            filters (Dict[str, Any], optional): Filters in JSON format that will be passed to API body. Defaults to {}.
            nrows (int, optional): Max number of rows to pull during execution. Defaults to 1000.

        Returns:
            pd.DataFrame: Output dataframe.
        """

        hubspot = Hubspot(credentials=self.hubspot_credentials)

        url = hubspot.get_api_url(
            endpoint=endpoint,
            properties=properties,
            filters=filters,
        )

        if filters:
            filters_formatted = self.format_filters(filters=filters)
            body = hubspot.get_api_body(filters=filters_formatted)
            self.method = "POST"
            partition = hubspot.to_json(url=url, body=body, method=self.method)
            full_dataset = partition["results"]

            while "paging" in partition.keys() and len(full_dataset) < nrows:
                body = json.loads(hubspot.get_api_body(filters=filters_formatted))
                body["after"] = partition["paging"]["next"]["after"]
                partition = hubspot.to_json(
                    url=url, body=json.dumps(body), method=self.method
                )
                full_dataset.extend(partition["results"])

        else:
            self.method = "GET"
            partition = hubspot.to_json(url=url, method=self.method)
            full_dataset = partition[list(partition.keys())[0]]

            offset_type = self.get_offset_from_response(partition)[0]
            offset_value = self.get_offset_from_response(partition)[1]

            while offset_value and len(full_dataset) < nrows:
                url = hubspot.get_api_url(
                    endpoint=endpoint,
                    properties=properties,
                    filters=filters,
                )
                url += f"{offset_type}={offset_value}"

                partition = hubspot.to_json(url=url, method=self.method)
                full_dataset.extend(partition[list(partition.keys())[0]])

                offset_type = self.get_offset_from_response(partition)[0]
                offset_value = self.get_offset_from_response(partition)[1]

        df = self.to_df(full_dataset)[:nrows]

        return df
