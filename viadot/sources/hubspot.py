import json
from typing import Any, Dict, List

from viadot.exceptions import CredentialError
from viadot.sources.base import Source
from viadot.utils import handle_api_response


class Hubspot(Source):
    """
    A class that connects and extracts data from Hubspot API. Documentation is available under https://developers.hubspot.com/docs/api/crm/understanding-the-crm.
    Connector allows to pull data in three ways:
        - using base API for crm schemas as an endpoint (eg. "contacts", ""line_items", "deals", ...),
        - using full url as endpoint.
    """

    def __init__(self, credentials: dict, *args, **kwargs):
        """
        Create an instance of Hubspot.

        Args:
            credentials (dict): Credentials to Hubspot API.
        """

        self.credentials = credentials
        if credentials is not None:
            try:
                self.headers = {
                    "Authorization": f'Bearer {self.credentials["TOKEN"]}',
                    "Content-Type": "application/json",
                }
            except:
                raise CredentialError("Credentials not found.")

        super().__init__(*args, credentials=self.credentials, **kwargs)

        self.base_url = self.credentials.get("URL")

    def clean_special_characters(self, value: str = None) -> str:
        """
        Function that maps special characters from ASCII to HTML5 equivalents.

        Args:
            value (str, optional): Value for decoding. Defaults to None.

        Returns:
            str: Value after decoding to HTML5.
        """

        mapping = {
            "!": "%21",
            '"': "%22",
            "#": "%23",
            "$": "%24",
            "%": "%25",
            "&": "%26",
            "(": "%28",
            ")": "%29",
            "*": "%2A",
            "+": "%2B",
            ",": "%2C",
        }

        for key in mapping.keys():
            if key in value:
                value = value.replace(key, mapping[key])

        return value

    def get_properties_url(self, endpoint: str = None) -> str:
        """
        Function that generates URL with all properties ( = detailed information about all avaliable columns for defined endpoint (schema)).

        Args:
            endpoint (str, optional): Hubspot schema from crm objects (eg. contacts, line_items, ...). Defaults to None.

        Returns:
            str: Url with properties for defined endpoint.
        """

        url = f"{self.base_url}/properties/v2/{endpoint}/properties"
        return url

    def get_api_url(
        self,
        endpoint: str = None,
        filters: Dict[str, Any] = None,
        properties: List[Any] = None,
    ) -> str:
        """
        Function that generates full url for Hubspot API with defined parametrs.

        Args:
            endpoint (str, optional): Schema or full url. Defaults to None.
            filters (Dict[str, Any], optional): Filters defined for the API body in specific order. Defaults to None.
            properties (List[Any], optional): List of user-defined columns to be pulled from the API. Defaults to None.

        Returns:
            str: Generated url passed to Hubspot API.
        """

        if self.base_url in endpoint:
            url = endpoint
        else:
            if endpoint.startswith("hubdb"):
                url = f"{self.base_url}/{endpoint}"
            else:
                if filters:
                    url = (
                        f"{self.base_url}/crm/v3/objects/{endpoint}/search/?limit=100&"
                    )
                else:
                    url = f"{self.base_url}/crm/v3/objects/{endpoint}/?limit=100&"

                if properties and len(properties) > 0:
                    url += f'properties={",".join(properties)}&'

        return url

    def get_api_body(self, filters: Dict[str, Any] = {}) -> Dict:
        """
        Function that cleans the filters body and converts to a JSON formatted value.

        Args:
            filters (Dict[str, Any], optional): Filters dictionary that will be passed to Hubspot API. Defaults to {}.

                    Example below:

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
                    [IN, NOT_HAS_PROPERTY, LT, EQ, GT, NOT_IN, GTE, CONTAINS_TOKEN, HAS_PROPERTY, LTE, NOT_CONTAINS_TOKEN, BETWEEN, NEQ]

                    LT - Less than
                    LTE - Less than or equal to
                    GT - Greater than
                    GTE - Greater than or equal to
                    EQ - Equal to
                    NEQ - Not equal to
                    BETWEEN - Within the specified range. In your request, use key-value pairs to set highValue and value. Refer to the example above.
                    IN - Included within the specified list. This operator is case-sensitive, so inputted values must be in lowercase.
                    NOT_IN - Not included within the specified list
                    HAS_PROPERTY - Has a value for the specified property
                    NOT_HAS_PROPERTY - Doesn't have a value for the specified property
                    CONTAINS_TOKEN - Contains a token. In your request, you can use wildcards (*) to complete a partial search. For example, use the value *@hubspot.com to retrieve contacts with a HubSpot email address.
                    NOT_CONTAINS_TOKEN  -Doesn't contain a token

        Returns:
            Dict: Filters with a JSON format.
        """
        payload = json.dumps({"filterGroups": filters, "limit": 100})

        return payload

    def to_json(self, url: str = None, body: str = None, method: str = None) -> Dict:
        """
        Function that converts API response to a JSON formatted data.

        Args:
            url (str, optional): Hubspot API url. Defaults to None.
            body (str, optional): Filters that will be pushed to the API body. Defaults to None.
            method (str, optional): Method of the API call ("GET"/"POST"). Defaults to None.

        Returns:
            Dict: API response in JSON format.
        """

        response = handle_api_response(
            url=url, headers=self.headers, body=body, method=method
        )

        return response.json()
