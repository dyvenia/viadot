from datetime import datetime
from typing import Any, Dict, Literal

import pandas as pd
from prefect.utilities import logging

from viadot.config import local_config
from viadot.exceptions import APIError, CredentialError
from viadot.sources.base import Source
from viadot.utils import handle_api_response

logger = logging.get_logger()


class CustomerGauge(Source):
    API_URL = "https://api.eu.customergauge.com/v7/rest/sync/"

    def __init__(
        self,
        endpoint: Literal["responses", "non-responses"] = None,
        url: str = None,
        credentials: Dict[str, Any] = None,
    ):
        """
        A class to connect and download data using Customer Gauge API.
        Below is the documentation for each of this API's gateways:
            Responses gateway https://support.customergauge.com/support/solutions/articles/5000875861-get-responses
            Non-Responses gateway https://support.customergauge.com/support/solutions/articles/5000877703-get-non-responses

        Args:
            endpoint (Literal["responses", "non-responses"]): Indicate which endpoint to connect. Defaults to None.
            url (str, optional): Endpoint URL. Defaults to None.
            credentials (Dict[str, Any], optional): Credentials to connect with API containing client_id, client_secret. Defaults to None.

        Raises:
            ValueError: If endpoint is not provided or incorect.
            CredentialError: If credentials are not provided in local_config or directly as a parameter
        """
        self.endpoint = endpoint
        if endpoint is not None:
            if endpoint in ["responses", "non-responses"]:
                self.url = f"{self.API_URL}{endpoint}"
            else:
                raise ValueError(
                    "Incorrect endpoint name. Choose: 'responses' or 'non-responses'"
                )
        elif url is not None:
            self.url = url
        else:
            raise ValueError(
                "Provide endpoint name. Choose: 'responses' or 'non-responses'. Otherwise, provide URL"
            )

        if credentials is not None:
            self.credentials = credentials
        else:
            self.credentials = local_config.get("CustomerGauge")
            if self.credentials is None:
                raise CredentialError("Credentials not provided.")

        super().__init__(credentials=self.credentials)

    def get_token(self) -> str:
        """
        Gets Bearer Token using POST request method.

        Raises:
            APIError: If token is not returned.

        Returns:
            str: Bearer Token value.
        """
        url = "https://auth.EU.customergauge.com/oauth2/token"
        client_id = self.credentials.get("client_id", None)
        client_secret = self.credentials.get("client_secret", None)

        headers = {"Content-Type": "application/x-www-form-urlencoded"}
        body = {
            "grant_type": "client_credentials",
            "client_id": client_id,
            "client_secret": client_secret,
        }
        api_response = handle_api_response(
            url=url, params=body, headers=headers, method="POST"
        )
        token = api_response.json().get("access_token")

        if token is None:
            raise APIError("The token could not be generated. Check your credentials.")

        return token

    def get_json_response(
        self,
        cursor: int = None,
        pagesize: int = 1000,
        date_field: Literal[
            "date_creation", "date_order", "date_sent", "date_survey_response"
        ] = None,
        start_date: datetime = None,
        end_date: datetime = None,
    ) -> Dict[str, Any]:
        """
        Gets JSON with nested structure that contains data and cursor parameter value using GET request method.

        Args:
            cursor (int, optional): Cursor value to navigate to the page. Defaults to None.
            pagesize (int, optional): Number of responses (records) returned per page, max value = 1000. Defaults to 1000. Defaults to 1000.
            date_field (Literal["date_creation", "date_order", "date_sent", "date_survey_response"], optional): Specifies the date type which filter date range. Defaults to None.
            start_date (datetime, optional): Defines the period start date in yyyy-mm-dd format. Defaults to None.
            end_date (datetime, optional): Defines the period end date in yyyy-mm-dd format. Defaults to None.

        Raises:
            ValueError: If at least one date argument were provided and the rest is missing. Needed all 3 or skip them.
            APIError: If no response from API call.

        Returns:
            Dict[str, Any]: JSON with data and cursor parameter value.
        """
        url = self.url

        params = {
            "per_page": pagesize,
            "with[]": ["drivers", "tags", "questions", "properties"],
            "cursor": cursor,
        }

        if any([date_field, start_date, end_date]):
            if all([date_field, start_date, end_date]):
                params["period[field]"] = (date_field,)
                params["period[start]"] = (start_date,)
                params["period[end]"] = end_date
            else:
                raise ValueError(
                    "Missing date arguments: 'date_field', 'start_date', 'end_date'. Provide all 3 arguments or skip all of them."
                )

        header = {"Authorization": f"Bearer {self.get_token()}"}
        api_response = handle_api_response(url=url, headers=header, params=params)
        response = api_response.json()

        if response is None:
            raise APIError("No response.")
        return response

    def get_cursor(self, json_response: Dict[str, Any] = None) -> int:
        """
        Returns cursor value that is needed to navigate to the next page in the next API call for specific pagesize.

        Args:
            json_response (Dict[str, Any], optional): Dictionary with nested structure that contains data and cursor parameter value. Defaults to None.

        Raises:
            ValueError: If cursor value not found.

        Returns:
            int: Cursor value.
        """

        try:
            cur = json_response["cursor"]["next"]
        except:
            raise ValueError(
                "Provided argument doesn't contain 'cursor' value. Pass json returned from the endpoint."
            )

        return cur
