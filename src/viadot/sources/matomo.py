"""Source for ingesting data from Matomo API."""

from typing import Any, Literal

import pandas as pd
from pydantic import BaseModel
import requests

from viadot.config import get_source_credentials
from viadot.sources.base import Source
from viadot.utils import add_viadot_metadata_columns, validate


HTTP_STATUS_OK = 200


class MatomoCredentials(BaseModel):
    """Check Matomo credentials dict.

    One key is required: api_token_auth.
        - api_token: The unique authentication api_token for the Matomo API.

    Args:
         BaseModel (pydantic.BaseModel): Base class for data validation.
    """

    api_token: str


class Matomo(Source):
    """Matomo source class for fetching data from Matomo API."""

    def __init__(
        self,
        credentials: MatomoCredentials | None = None,
        config_key: str = "matomo",
        *args,
        **kwargs,
    ):
        """Initialize Matomo source with credentials or config key.

        Connector allows to pull data from Matomo API and convert it to a pandas
            DataFrame.

        Args:
            - credentials: MatomoCredentials object or a dict with api_token key.
            - config_key: The key in the viadot config holding relevant credentials.
        Usage:
            matomo = Matomo(credentials=matomo_credentials)

        Example:
            matomo = Matomo(api_token="YOUR_api_token")
            df = matomo.to_df()
        """
        raw_creds = credentials or get_source_credentials(config_key)
        validated_creds = dict(MatomoCredentials(**raw_creds))
        super().__init__(*args, credentials=validated_creds, **kwargs)

        self.data = None

    def fetch_data(
        self,
        api_token: str,
        url: str,
        params: dict[str, str],
    ) -> None:
        """Connect to Matomo API and fetch api response data as JSON.

        The function sends a GET request to the Matomo API endpoint with the specified
        parameters, including the authentication api_token.
        If the request is successful,the response data is stored in the `data` attribute
        of the Matomo instance.If the request fails, a ValueError is raised
            with the error message.

        Args:
            api_token (str): The authentication api_token for the Matomo API.
            url (str): The base URL of the Matomo instance.
            params (dict[str, str]): Parameters for the API request.
                Required params: "module","method","idSite","period","date","format".
        """
        if not params:
            params = {}

        params["token_auth"] = api_token
        response = requests.get(f"{url}/index.php", params=params, timeout=30)
        # TODO check also response text for error message.
        if response.status_code == HTTP_STATUS_OK:
            self.data = response.json()
        else:
            msg = f"Failed to fetch data: {response.text}"
            raise ValueError(msg)

    @add_viadot_metadata_columns
    def to_df(
        self,
        top_level_fields: list[str],
        record_path: str | list[str],
        record_prefix: str | None = None,
        if_empty: Literal["warn", "skip", "fail"] = "warn",
        tests: dict[str, Any] | None = None,
    ) -> pd.DataFrame:
        """Convert Matomo data to pandas DataFrame.

        Args:
            top_level_fields (list[str]): List of top lvl fields to get from
                the response JSON.
            record_path (str or list[str]): The path field to the records in the
                response JSON
                Could be handled as a list of path + fields to extract:
                        record_path = 'actionDetails'
                        record_path = ['actionDetails', 'eventAction']
            record_prefix = A prefix for the record path fields.For example:"action_"
            if_empty: What to do if no data is available.
                     Defaults to "warn".
            tests (Dict[str], optional): A dictionary with optional list of tests
                to verify the output dataframe. If defined, triggers the `validate`
                function from utils. Defaults to None.

        Returns:
            pd.DataFrame: DataFrame containing Matomo data.

        Raises:
            ValueError: If no data has been fetched yet.
        """
        if self.data is None:
            msg = "No data available. Call fetch_data() first."
            raise ValueError(msg)

        df = pd.json_normalize(
            self.data,
            record_path=record_path,
            meta=top_level_fields,
            sep="_",
            errors="ignore",  # ignore puts nan value when field is missing in a record
            record_prefix=record_prefix,
        )

        if df.empty:
            self._handle_if_empty(if_empty=if_empty)

        if tests:
            validate(df=df, tests=tests)

        return df
