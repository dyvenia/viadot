"""Mindful API connector."""

from datetime import date, timedelta
from io import StringIO
import json
from typing import Any, Literal

import pandas as pd
from pydantic import BaseModel
from requests.auth import HTTPBasicAuth
from requests.models import Response

from viadot.config import get_source_credentials
from viadot.exceptions import APIError, CredentialError
from viadot.sources.base import Source
from viadot.utils import add_viadot_metadata_columns, handle_api_response


class MindfulCredentials(BaseModel):
    """Checking for values in Mindful credentials dictionary.

    Two key values are held in the Mindful connector:
        - customer_uuid: The unique ID for the organization.
        - auth_token: A unique token to be used as the password for API requests.

    Args:
        BaseModel (pydantic.main.ModelMetaclass): A base class for creating
            Pydantic models.
    """

    customer_uuid: str
    auth_token: str


class Mindful(Source):
    """Class implementing the Mindful API.

    Documentation for this API is available at: https://apidocs.surveydynamix.com/.
    """

    ENDPOINTS = ("interactions", "responses", "surveys")

    def __init__(
        self,
        *args,
        credentials: MindfulCredentials | None = None,
        config_key: str = "mindful",
        region: Literal["us1", "us2", "us3", "ca1", "eu1", "au1"] = "eu1",
        **kwargs,
    ):
        """Create a Mindful instance.

        Args:
            credentials (Optional[MindfulCredentials], optional): Mindful credentials.
                Defaults to None.
            config_key (str, optional): The key in the viadot config holding relevant
                credentials. Defaults to "mindful".
            region (Literal[us1, us2, us3, ca1, eu1, au1], optional): Survey Dynamix
                region from where to interact with the mindful API. Defaults to "eu1"
                English (United Kingdom).

        Examples:
            mindful = Mindful(
                credentials=credentials,
                config_key=config_key,
                region=region,
            )
            mindful.api_connection(
                endpoint=endpoint,
                date_interval=date_interval,
                limit=limit,
            )
            data_frame = mindful.to_df()
        """
        credentials = credentials or get_source_credentials(config_key) or None
        if credentials is None:
            msg = "Missing credentials."
            raise CredentialError(msg)

        validated_creds = dict(MindfulCredentials(**credentials))
        super().__init__(*args, credentials=validated_creds, **kwargs)

        self.auth = (credentials["customer_uuid"], credentials["auth_token"])
        if region != "us1":
            self.region = f"{region}."
        else:
            self.region = ""

    def _mindful_api_response(
        self,
        params: dict[str, Any] | None = None,
        endpoint: str = "",
    ) -> Response:
        """Call to Mindful API given an endpoint.

        Args:
            params (Optional[Dict[str, Any]], optional): Parameters to be passed into
                the request. Defaults to None.
            endpoint (str, optional): API endpoint for an individual request.
                Defaults to "".

        Returns:
            Response: request object with the response from the Mindful API.
        """
        return handle_api_response(
            url=f"https://{self.region}surveydynamix.com/api/{endpoint}",
            params=params,
            method="GET",
            auth=HTTPBasicAuth(*self.auth),
        )

    def api_connection(
        self,
        endpoint: Literal["interactions", "responses", "surveys"] = "surveys",
        date_interval: list[date] | None = None,
        limit: int = 1000,
    ) -> None:
        """General method to connect to Survey Dynamix API and generate the response.

        Args:
            endpoint (Literal["interactions", "responses", "surveys"], optional): API
                endpoint for an individual request. Defaults to "surveys".
            date_interval (Optional[List[date]], optional): Date time range detailing
                the starting date and the ending date. If no range is passed, one day of
                data since this moment will be retrieved. Defaults to None.
            limit (int, optional): The number of matching interactions to return.
                Defaults to 1000.

        Raises:
            ValueError: Not available endpoint.
            APIError: Failed to download data from the endpoint.
        """
        if endpoint not in self.ENDPOINTS:
            raise ValueError(
                f"Survey Dynamix endpoint: '{endpoint}',"
                + " is not available through Mindful viadot connector."
            )

        if (
            date_interval is None
            or all(list(map(isinstance, date_interval, [date] * len(date_interval))))
            is False
        ):
            reference_date = date.today()
            date_interval = [reference_date - timedelta(days=1), reference_date]

            self.logger.warning(
                "No `date_interval` parameter was defined, or was erroneously "
                + "defined. `date_interval` parameter must have the folloing "
                + "structure:\n\t[`date_0`, `date_1`], having that `date_1` > "
                + "`date_0`.\nBy default, one day of data, from "
                + f"{date_interval[0].strftime('%Y-%m-%d')} to "
                + f"{date_interval[1].strftime('%Y-%m-%d')}, will be obtained."
            )

        params = {
            "_limit": limit,
            "start_date": f"{date_interval[0]}",
            "end_date": f"{date_interval[1]}",
        }

        if endpoint == "surveys":
            del params["start_date"]
            del params["end_date"]

        response = self._mindful_api_response(
            endpoint=endpoint,
            params=params,
        )
        response_ok = 200
        no_data_code = 204
        if response.status_code == response_ok:
            self.logger.info(
                f"Successfully downloaded '{endpoint}' data from mindful API."
            )
            self.data = StringIO(response.content.decode("utf-8"))
        elif response.status_code == no_data_code and not response.content.decode():
            self.logger.warning(
                f"There are not '{endpoint}' data to download from"
                + f" {date_interval[0]} to {date_interval[1]}."
            )
            self.data = json.dumps({})
        else:
            self.logger.error(
                f"Failed to downloaded '{endpoint}' data. - {response.content}"
            )
            msg = f"Failed to downloaded '{endpoint}' data."
            raise APIError(msg)

    @add_viadot_metadata_columns
    def to_df(
        self,
        if_empty: str = "warn",
    ) -> pd.DataFrame:
        """Download the data to a pandas DataFrame.

        Args:
            if_empty (str, optional): What to do if a fetch produce no data.
                Defaults to "warn

        Returns:
            pd.Dataframe: The response data as a pandas DataFrame plus viadot metadata.
        """
        super().to_df(if_empty=if_empty)

        data_frame = pd.read_json(self.data)

        if data_frame.empty:
            self._handle_if_empty(
                if_empty=if_empty,
                message="The response does not contain any data.",
            )
        else:
            self.logger.info("Successfully downloaded data from the Mindful API.")

        return data_frame
