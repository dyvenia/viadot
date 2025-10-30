"""Source for ingesting data from Matomo API."""

from typing import Any, Literal

import pandas as pd
from pydantic import BaseModel

from viadot.config import get_source_credentials
from viadot.exceptions import APIError
from viadot.sources.base import Source
from viadot.utils import add_viadot_metadata_columns, handle_api_response, validate


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

    def _validate_parameters(
        self, api_token: str, url: str, params: dict[str, str]
    ) -> dict[str, str]:
        """Validate input parameters and return processed params.

        Args:
            api_token (str): The authentication token for the Matomo API.
            url (str): The base URL of the Matomo instance.
            params (dict[str, str]): Parameters for the API request.

        Returns:
            dict[str, str]: The processed parameters with the api_token added.

        Raises:
            ValueError: If api_token or url are empty, or if required parameters
                are missing.
        """
        if not api_token:
            msg = "api_token is required and cannot be empty"
            self.logger.error(msg)
            raise ValueError(msg)

        if not url:
            msg = "url is required and cannot be empty"
            self.logger.error(msg)
            raise ValueError(msg)

        if not params:
            params = {}

        # Validate essential API parameters
        required_params = ["module", "method", "idSite", "period", "date", "format"]
        missing_params = [
            param
            for param in required_params
            if param not in params or not params[param]
        ]
        if missing_params:
            msg = f"Missing required API parameters: {missing_params}"
            self.logger.error(msg)
            raise ValueError(msg)

        params["token_auth"] = api_token
        return params

    def _validate_response(self, response: object) -> dict[str, Any]:
        """Process and validate API response.

        Args:
            response (object): The HTTP response object from the Matomo API request.

        Returns:
            dict[str, Any]: The parsed JSON response data.

        Raises:
            APIError: If the response status is not OK, JSON parsing fails, or
                the response data is not in expected format (list or dict).
        """
        if response.status_code == HTTP_STATUS_OK:
            try:
                data = response.json()
                self.logger.info("Successfully fetched data from Matomo API.")

                # Validate that we have a proper response structure
                if not isinstance(data, list | dict):
                    msg = "API response is not in expected JSON format (list or dict)"
                    self.logger.error(msg)
                    raise APIError(msg)

                # Log response summary
                if isinstance(data, list):
                    self.logger.info(f"API returned {len(data)} records.")
                elif isinstance(data, dict):
                    self.logger.info(
                        f"API returned response with keys: {list(data.keys())}"
                    )

                return data

            except ValueError as e:
                msg = "Failed to parse API response as JSON"
                self.logger.exception(msg)
                raise APIError(msg) from e
        else:
            msg = f"Failed to fetch data from Matomo API: {response.status_code} - {response.text}"
            self.logger.exception(msg)
            raise APIError(msg) from None

    def fetch_data(
        self,
        api_token: str,
        url: str,
        params: dict[str, str],
    ) -> None:
        """Connect to Matomo API and fetch api response data as JSON.

        The function sends a GET request to the Matomo API endpoint with the specified
        parameters, including the authentication api_token.
        If the request is successful, the response data is stored in the `data`
        attribute of the Matomo instance. If the request fails, an APIError is
        raised with the error message.

        Args:
            api_token (str): The authentication api_token for the Matomo API.
            url (str): The base URL of the Matomo instance.
            params (dict[str, str]): Parameters for the API request.
                Required params: "module","method","idSite","period","date","format".

        Raises:
            ValueError: If required parameters are missing or invalid.
        """
        params = self._validate_parameters(api_token, url, params)

        try:
            response = handle_api_response(
                url=f"{url}/index.php",
                params=params,
                method="GET",
            )
            self.data = self._validate_response(response)
        except Exception as e:
            msg = "Failed to fetch data from Matomo API"
            self.logger.exception(msg)
            raise APIError(msg) from e

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

        self.logger.info("Converting Matomo data to pandas DataFrame.")

        # Validate record_path exists in data if it's a dict
        if isinstance(self.data, dict) and record_path not in self.data:
            self.logger.warning(
                f"record_path '{record_path}' not found in response data."
            )
            self.logger.warning(f"Available keys in response: {list(self.data.keys())}")

        df = pd.json_normalize(
            self.data,
            record_path=record_path,
            meta=top_level_fields,
            sep="_",
            errors="ignore",  # ignore puts nan value when field is missing in a record
            record_prefix=record_prefix,
        )

        if df.empty:
            self.logger.warning("No records found in the specified record_path.")
            self._handle_if_empty(if_empty=if_empty)

        if tests:
            self.logger.info("Running data validation tests.")
            validate(df=df, tests=tests)

        self.logger.info(f"Successfully processed {len(df)} records from Matomo data.")

        return df

    def to_json(self) -> dict[str, Any]:
        """Return the fetched data as a dictionary.

        Returns:
            dict[str, Any]: The Matomo API response data.

        Raises:
            ValueError: If no data has been fetched yet.
        """
        if self.data is None:
            msg = "No data available. Call fetch_data() first."
            self.logger.error(msg)
            raise ValueError(msg)

        self.logger.info("Returning Matomo data as JSON.")
        return self.data
