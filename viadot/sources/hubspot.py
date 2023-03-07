import json

from typing import Any, Dict

from prefect.utilities import logging
from viadot.config import local_config
from viadot.sources.base import Source
from viadot.utils import handle_api_response


class Hubspot(Source):
    """
    A class implementing the Hubspot API.

    Parameters
    ----------
    query_params : Dict[str, Any], optional
        The parameters to pass to the GET query.
    """

    def __init__(self, *args, **kwargs):

        DEFAULT_CREDENTIALS = local_config.get("HUBSPOT")
        credentials = kwargs.pop("credentials", DEFAULT_CREDENTIALS)
        if credentials is None:
            raise CredentialError("Missing credentials.")
        super().__init__(*args, credentials=credentials, **kwargs)

        self.API_ENDPOINT = DEFAULT_CREDENTIALS.get("URL")

        # self.query_params = query_params
        # self.endpoint = query_params.get('endpoint')
        # self.filter = query_params.get('filter')
        # self.properties = query_params.get('properties')

    def get_query_params_keys(self, params: Dict[str, Any] = None):
        self.query_params = params
        return self.query_params

    def get_single_property_url(
        self, endpoint: str = None, properties: Dict[str, Any] = None
    ):
        url = f"{self.API_ENDPOINT}/crm/v3/objects/{endpoint}/search/?limit=100&"

        if len(properties) > 0:
            property_list = properties
            url += f'properties={",".join(property_list)}&='

        return url

    def get_all_properties_url(self):
        url = f"{self.API_ENDPOINT}/properties/v2/{self.endpoint}/properties"
        return url

    def get_api_body(self, filters: Dict[str, Any] = {}):

        """

        [IN, NOT_HAS_PROPERTY, LT, EQ, GT, NOT_IN, GTE, CONTAINS_TOKEN, HAS_PROPERTY, LTE, NOT_CONTAINS_TOKEN, BETWEEN, NEQ]

            OPERATOR	DESCRIPTION
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

        """

        payload = json.dumps({"filterGroups": filters, "limit": 100})

        return payload

    def to_json(self, url: str = None, body: str = None) -> Dict[str, Any]:
        headers = {
            "Authorization": f'Bearer {self.credentials["TOKEN"]}',
            "Content-Type": "application/json",
        }
        response = handle_api_response(
            url=url, headers=headers, body=body, method="POST"
        )

        return response.json()
