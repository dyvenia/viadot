"""Source for connecting to Supermetrics API."""

import json
from typing import Any

import numpy as np
import pandas as pd
from pydantic import BaseModel

from viadot.config import get_source_credentials
from viadot.sources.base import Source
from viadot.utils import add_viadot_metadata_columns, handle_api_response


class SupermetricsCredentials(BaseModel):
    """Represents credentials for accessing the Supermetrics API.

    This class encapsulates the necessary credentials required to authenticate
    and access the Supermetrics API.

    Attributes:
    ----------
        user (str):
            The email account associated with the Supermetrics user.
        api_key (str):
            The API key that provides access to the Supermetrics API.

    """

    user: str
    api_key: str


class Supermetrics(Source):
    """Implements methods for querying and interacting with the Supermetrics API.

    This class provides functionality to query data from the Supermetrics API,
    which is a tool used for accessing data from various data sources. The API
    documentation can be found at:
    https://supermetrics.com/docs/product-api-getting-started/. For information
    on usage limits, please refer to:
    https://supermetrics.com/docs/product-api-usage-limits/.

    Args:
    ----
        config_key (str, optional):
            The key in the viadot configuration that holds the relevant credentials
            for the API. Defaults to None.
        credentials (dict of str to any, optional):
            A dictionary containing the credentials needed for the API connection
            configuration, specifically `api_key` and `user`. Defaults to None.
        query_params (dict of str to any, optional):
            A dictionary containing the parameters to pass to the GET query.
            These parameters define the specifics of the data request. Defaults to None.
            For a full specification of possible parameters, see:
            https://supermetrics.com/docs/product-api-get-data/.

    """

    BASE_URL = "https://api.supermetrics.com/enterprise/v2/query/data/json"

    def __init__(
        self,
        *args,
        credentials: dict[str, Any] | None = None,
        config_key: str | None = None,
        query_params: dict[str, Any] | None = None,
        **kwargs,
    ) -> None:
        """Initialize the Supermetrics object.

        This constructor sets up the necessary components to interact with the
        Supermetrics API, including the credentials and any query parameters.

        Args:
        ----
            credentials (SupermetricsCredentials, optional):
                An instance of `SupermetricsCredentials` containing the API key and
                user email for authentication. Defaults to None.
            config_key (str, optional):
                The key in the viadot configuration that holds the relevant credentials
                for the API. Defaults to None.
            query_params (dict of str to any, optional):
                A dictionary containing the parameters to pass to the GET query. These
                parameters define the specifics of the data request. Defaults to None.

        """
        raw_creds = credentials or get_source_credentials(config_key)
        validated_creds = dict(SupermetricsCredentials(**raw_creds))

        super().__init__(*args, credentials=validated_creds, **kwargs)

        self.query_params = query_params

    def to_json(self, timeout: tuple = (3.05, 60 * 30)) -> dict[str, Any]:
        """Download query results to a dictionary.

        This method executes the query against the Supermetrics API and retrieves
        the results as a JSON dictionary.

        Args:
        ----
            timeout (tuple of float, optional):
                A tuple specifying the timeout values for the request. The first value
                is the timeout for connection issues, and the second value is
                the timeout for query execution. Defaults to (3.05, 1800), which
                provides a short timeout for connection issues and a longer timeout
                for the query execution.

        Returns:
        -------
            dict:
                The response from the Supermetrics API, returned as a JSON dictionary.

        Raises:
        ------
            ValueError:
                Raised if the query parameters are not set before calling this method.

        """
        if not self.query_params:
            msg = "Please build the query first"
            raise ValueError(msg)

        params = {"json": json.dumps(self.query_params)}
        headers = {"Authorization": f"Bearer {self.credentials.get('api_key')}"}

        response = handle_api_response(
            url=Supermetrics.BASE_URL,
            params=params,
            headers=headers,
            timeout=timeout,
        )
        return response.json()

    @classmethod
    def _get_col_names_google_analytics(
        cls,
        response: dict,
    ) -> list[str]:
        """Get column names from Google Analytics data.

        This method extracts the column names from the JSON response received
        from a Google Analytics API call.

        Args:
        ----
            response (dict):
                A dictionary containing the JSON response from the API call.

        Returns:
        -------
            list of str:
                A list of column names extracted from the Google Analytics data.

        Raises:
        ------
            ValueError:
                Raised if no data is returned in the response or if the column names
                cannot be determined.

        """
        is_pivoted = any(
            field["field_split"] == "column"
            for field in response["meta"]["query"]["fields"]
        )

        if is_pivoted:
            if not response["data"]:
                msg = "Couldn't find column names as query returned no data."
                raise ValueError(msg)
            columns = response["data"][0]
        else:
            cols_meta = response["meta"]["query"]["fields"]
            columns = [col_meta["field_name"] for col_meta in cols_meta]
        return columns

    @classmethod
    def _get_col_names_other(cls, response: dict) -> list[str]:
        """Get column names from non-Google Analytics data.

        This method extracts the column names from the JSON response received
        from an API call that is not related to Google Analytics.

        Args:
        ----
            response (dict):
                A dictionary containing the JSON response from the API call.

        Returns:
        -------
            list of str:
                A list of column names extracted from the non-Google Analytics data.

        """
        cols_meta = response["meta"]["query"]["fields"]
        return [col_meta["field_name"] for col_meta in cols_meta]

    def _get_col_names(self) -> list[str]:
        """Get column names based on the data type.

        This method determines the appropriate column names for the data based
        on its type, whether it's Google Analytics data or another type.

        Returns:
        -------
            list of str:
                A list of column names based on the data type.

        Raises:
        ------
            ValueError:
                Raised if the column names cannot be determined.

        """
        response: dict = self.to_json()
        if self.query_params["ds_id"] == "GA":
            return Supermetrics._get_col_names_google_analytics(response)

        return Supermetrics._get_col_names_other(response)

    @add_viadot_metadata_columns
    def to_df(
        self,
        if_empty: str = "warn",
        query_params: dict[str, Any] | None = None,
    ) -> pd.DataFrame:
        """Download data into a pandas DataFrame.

        This method retrieves data from the Supermetrics API and loads it into
        a pandas DataFrame.

        Args:
        ----
            if_empty (str, optional):
                Specifies the action to take if the query returns no data.
                Options include "fail" to raise an error or "ignore" to return
                an empty DataFrame. Defaults to "fail".

        Returns:
        -------
            pd.DataFrame:
                A pandas DataFrame containing the JSON data retrieved from the API.

        Raises:
        ------
            ValueError:
                Raised if the DataFrame is empty and `if_empty` is set to "fail".

        """
        # Use provided query_params or default to the instance's query_params
        if query_params is not None:
            self.query_params = query_params

        if not self.query_params:
            msg = "Query parameters are required to fetch data."
            raise ValueError(msg)

        self.query_params["api_key"] = self.credentials.get("api_key")

        try:
            columns = self._get_col_names()
        except ValueError:
            columns = None

        data = self.to_json()["data"]

        if data:
            df = pd.DataFrame(data[1:], columns=columns).replace("", np.nan)
        else:
            df = pd.DataFrame(columns=columns)

        if df.empty:
            self._handle_if_empty(if_empty)

        return df
