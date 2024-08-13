import json
import logging
import re
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
from pydantic import BaseModel

from viadot.config import get_source_credentials
from viadot.exceptions import APIError, CredentialError
from viadot.sources.base import Source
from viadot.utils import add_viadot_metadata_columns, handle_api_response

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


class HUBSPOT_CREDENTIALS(BaseModel):
    """
    Description:
        Checking for values in Hubspot credentials dictionary.
        One key value is held in the Hubspot connector:
            - token: The unique string characters to be indentified.
    Args:
        BaseModel (pydantic.main.ModelMetaclass): A base class for creating Pydantic models.
    """

    token: str


class Hubspot(Source):
    """
    A class that connects and extracts data from Hubspot API. Documentation is available
        under https://developers.hubspot.com/docs/api/crm/understanding-the-crm.

    Connector allows to pull data in two ways:
        - using base API for crm schemas as an endpoint (eg. "contacts", ""line_items", "deals", ...),
        - using full url as endpoint.
    """

    API_URL = "https://api.hubapi.com"

    def __init__(
        self,
        credentials: Optional[HUBSPOT_CREDENTIALS] = None,
        config_key: str = "hubspot",
        *args,
        **kwargs,
    ):
        """
        Description:
            Create an instance of Hubspot.

        Args:
            credentials (Optional[HUBSPOT_CREDENTIALS], optional): Hubspot credentials.
                Defaults to None.
            config_key (str, optional): The key in the viadot config holding relevant credentials.
                Defaults to "hubspot".

        Raises:
            CredentialError: If credentials are not provided in local_config or directly as a parameter.
        """
        credentials = credentials or get_source_credentials(config_key) or None
        if credentials is None:
            raise CredentialError("Missing credentials.")
        self.credentials = credentials

        super().__init__(*args, credentials=self.credentials, **kwargs)

    def _date_to_unixtimestamp(self, date: Optional[str] = None) -> int:
        """
        Description:
            Function for date conversion from user defined "yyyy-mm-dd" to Unix Timestamp
            (SECONDS SINCE JAN 01 1970. (UTC)).
            For example: 1680774921 SECONDS SINCE JAN 01 1970. (UTC) -> 11:55:49 AM 2023-04-06.

        Args:
            date (Optional[str], optional): Input date in format "yyyy-mm-dd". Defaults to None.

        Returns:
            int: Number of seconds that passed since 1970-01-01 until "date".
        """

        clean_date = int(datetime.timestamp(datetime.strptime(date, "%Y-%m-%d")) * 1000)

        return clean_date

    def _get_api_url(
        self,
        endpoint: Optional[str] = None,
        filters: Optional[Dict[str, Any]] = None,
        properties: Optional[List[Any]] = None,
    ) -> str:
        """
        Description:
            Generates full url for Hubspot API given filters and parameters.

        Args:
            endpoint (Optional[str], optional): API endpoint for an individual request.
                Defaults to None.
            filters (Optional[Dict[str, Any]], optional): Filters defined for the API body
                in specific order. Defaults to None.
            properties (Optional[List[Any]], optional): List of user-defined columns to be
                pulled from the API. Defaults to None.

        Returns:
            str: The final URL API.
        """
        if self.API_URL in endpoint:
            url = endpoint
        else:
            if endpoint.startswith("hubdb"):
                url = f"{self.API_URL}/{endpoint}"
            else:
                if filters:
                    url = f"{self.API_URL}/crm/v3/objects/{endpoint}/search/?limit=100&"
                else:
                    url = f"{self.API_URL}/crm/v3/objects/{endpoint}/?limit=100&"

                if properties and len(properties) > 0:
                    url += f'properties={",".join(properties)}&'

        return url

    def _format_filters(
        self,
        filters: Optional[List[Dict[str, Any]]],
    ) -> List[Dict[str, Any]]:
        """
        Description:
            API body (filters) conversion from a user defined to API language.
            Note: Right now only converts date to Unix Timestamp.

        Args:
            filters (Optional[List[Dict[str, Any]]]): List of filters in JSON format.

        Returns:
            List[Dict[str, Any]]: List of cleaned filters in JSON format.
        """
        for item in filters:
            for subitem in item["filters"]:
                for key in list(subitem.keys()):
                    lookup = subitem[key]
                    regex = re.findall("\d+-\d+-\d+", lookup)
                    if regex:
                        regex = self._date_to_unixtimestamp(lookup)
                        subitem[key] = f"{regex}"

        return filters

    def _get_api_body(self, filters: List[Dict[str, Any]]):
        """
        Description:
            Clean the filters body and converts to a JSON formatted value.

        Args:
            filters (List[Dict[str, Any]]): Filters dictionary that will be passed to Hubspot API.
                Defaults to {}.
                Example:
                    filters = {
                                "filters": [
                                    {
                                    "propertyName": "createdate",
                                    "operator": "BETWEEN",
                                    "highValue": "2023-03-27",
                                    "value": "2023-03-26"
                                    }
                                ]
                            }
                Operators between the min and max value are listed below:
                [IN, NOT_HAS_PROPERTY, LT, EQ, GT, NOT_IN, GTE, CONTAINS_TOKEN, HAS_PROPERTY,
                    LTE, NOT_CONTAINS_TOKEN, BETWEEN, NEQ]
                LT - Less than
                LTE - Less than or equal to
                GT - Greater than
                GTE - Greater than or equal to
                EQ - Equal to
                NEQ - Not equal to
                BETWEEN - Within the specified range. In your request, use key-value pairs
                    to set highValue and value. Refer to the example above.
                IN - Included within the specified list. This operator is case-sensitive,
                    so inputted values must be in lowercase.
                NOT_IN - Not included within the specified list
                HAS_PROPERTY - Has a value for the specified property
                NOT_HAS_PROPERTY - Doesn't have a value for the specified property
                CONTAINS_TOKEN - Contains a token. In your request, you can use wildcards (*)
                    to complete a partial search. For example, use the value *@hubspot.com
                    to retrieve contacts with a HubSpot email address.
                NOT_CONTAINS_TOKEN  -Doesn't contain a token

        Returns:
            Dict: Filters with a JSON format.
        """
        payload = json.dumps({"filterGroups": filters, "limit": 100})

        return payload

    def _api_call(
        self,
        url: Optional[str] = None,
        body: Optional[str] = None,
        method: Optional[str] = None,
    ) -> Dict:
        """
        Description:
            General method to connect to Hubspot API and generate the response.

        Args:
            url (Optional[str], optional): Hubspot API url. Defaults to None.
            body (Optional[str], optional): Filters that will be pushed to the API body.
                Defaults to None.
            method (Optional[str], optional): Method of the API call. Defaults to None.

        Returns:
            Dict: API response in JSON format.
        """

        headers = {
            "Authorization": f'Bearer {self.credentials["token"]}',
            "Content-Type": "application/json",
        }

        response = handle_api_response(
            url=url, headers=headers, data=body, method=method
        )

        return response.json()

    def _get_offset_from_response(
        self, api_response: Dict[str, Any]
    ) -> Optional[Tuple[str]]:
        """
        Description:
            Assign offset type/value depending on keys in API response.

        Args:
            api_response (Dict[str, Any]): API response in JSON format.

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

    def api_connection(
        self,
        endpoint: Optional[str] = None,
        filters: Optional[List[Dict[str, Any]]] = None,
        properties: Optional[List[Any]] = None,
        nrows: int = 1000,
    ):
        """
        Description:
            General method to connect to Hubspot API and generate the response.

        Args:
            endpoint (Optional[str], optional): API endpoint for an individual request.
                Defaults to None.
            filters (Optional[List[Dict[str, Any]]], optional): Filters defined for the API
                body in specific order. Defaults to None.
                Example:
                    filters=[
                        {
                            "filters": [
                                {
                                    "propertyName": "createdate",
                                    "operator": "BETWEEN",
                                    "highValue": "1642636800000",
                                    "value": "1641995200000",
                                },
                                {
                                    "propertyName": "email",
                                    "operator": "CONTAINS_TOKEN",
                                    "value": "*@xxxx.xx",
                                },
                            ]
                        }
                    ],
            properties (Optional[List[Any]], optional): List of user-defined columns to be
                pulled from the API. Defaults to None.
            nrows (int, optional): Max number of rows to pull during execution. Defaults to 1000.

        Raises:
            APIError: Failed to download data from the endpoint.
        """

        self.nrows = nrows

        url = self._get_api_url(
            endpoint=endpoint,
            filters=filters,
            properties=properties,
        )
        if filters:
            filters_formatted = self._format_filters(filters)
            body = self._get_api_body(filters=filters_formatted)
            method = "POST"
            partition = self._api_call(url=url, body=body, method=method)
            self.full_dataset = partition["results"]

            while "paging" in partition.keys() and len(self.full_dataset) < self.nrows:
                body = json.loads(self._get_api_body(filters=filters_formatted))
                body["after"] = partition["paging"]["next"]["after"]
                partition = self._api_call(
                    url=url, body=json.dumps(body), method=method
                )
                self.full_dataset.extend(partition["results"])

        else:
            method = "GET"
            partition = self._api_call(url=url, method=method)
            self.full_dataset = partition[list(partition.keys())[0]]

            offset_type, offset_value = self._get_offset_from_response(partition)

            while offset_value and len(self.full_dataset) < self.nrows:
                url = self._get_api_url(
                    endpoint=endpoint,
                    properties=properties,
                    filters=filters,
                )
                url += f"{offset_type}={offset_value}"

                partition = self._api_call(url=url, method=method)
                self.full_dataset.extend(partition[list(partition.keys())[0]])

                offset_type, offset_value = self._get_offset_from_response(partition)

    @add_viadot_metadata_columns
    def to_df(self) -> pd.DataFrame:
        """
        Description:
            Generate a Pandas Data Frame with the data in the Response object and metadata.

        Returns:
            pd.Dataframe: The response data as a Pandas Data Frame plus viadot metadata.
        """
        data_frame = pd.json_normalize(self.full_dataset)

        if data_frame.empty:
            self._handle_if_empty(
                if_empty="warn",
                message="The response does not contain any data.",
            )
        else:
            logger.info("Successfully downloaded data from the Mindful API.")

        return data_frame
