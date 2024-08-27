"""
This module defines classes and methods for interacting with the Supermetrics API.

The `SupermetricsCredentials` class handles credentials required to access the
Supermetrics API. The `Supermetrics` class provides methods to query and retrieve
data from the API.

Classes:
---------
- SupermetricsCredentials: Represents credentials for accessing the Supermetrics API.
- Supermetrics: Implements methods for querying and interacting with the
Supermetrics API.

Usage:
------
1. Instantiate `SupermetricsCredentials` with the required `user` and `api_key`.
2. Create a `Supermetrics` instance using the credentials and optional query
   parameters.
3. Use the `query` method to set query parameters and `to_df` to retrieve data
   as a pandas DataFrame.

API Documentation:
------------------
- API Documentation: https://supermetrics.com/docs/product-api-getting-started/
- Usage Limits: https://supermetrics.com/docs/product-api-usage-limits/
"""

import json
from typing import Dict, Any, List
import pandas as pd
import numpy as np

from pydantic import BaseModel

from ..config import get_source_credentials
from ..exceptions import CredentialError
from ..utils import handle_api_response
from .base import Source
from ..utils import add_viadot_metadata_columns


class SupermetricsCredentials(BaseModel):
    """
    Represents credentials for accessing the Supermetrics API.

    Attributes
    ----------
    user : str
        The Supermetrics user mail account.
    api_key : str
        The Supermetrics access API key.
    """

    user: str
    api_key: str


class Supermetrics(Source):
    """
    Implements methods for querying and interacting with the Supermetrics API.

    Documentation for this API is located at:
    https://supermetrics.com/docs/product-api-getting-started/
    Usage limits: https://supermetrics.com/docs/product-api-usage-limits/.

    Parameters
    ----------
    config_key : str, optional
        The key in the viadot config holding relevant credentials. Defaults to None.
    credentials : dict of str to any, optional
        Credentials for API connection configuration (`api_key` and `user`).
    query_params : dict of str to any, optional
        The parameters to pass to the GET query. Defaults to None.
        See https://supermetrics.com/docs/product-api-get-data/ for full specification.
    """

    API_ENDPOINT = "https://api.supermetrics.com/enterprise/v2/query/data/json"

    def __init__(
        self,
        *args,
        credentials: dict[str, Any] | None = None,
        config_key: str = None,
        query_params: Dict[str, Any] = None,
        **kwargs,
    ):
        """
        Initialize the Supermetrics object.

        Parameters
        ----------
        credentials : SupermetricsCredentials, optional
            Credentials for API connection configuration (`api_key` and `user`).
        config_key : str, optional
            The key in the viadot config holding relevant credentials. Defaults to None.
        query_params : dict of str to any, optional
            The parameters to pass to the GET query. Defaults to None.
        """
        credentials = credentials or get_source_credentials(config_key) or None

        if credentials is None or not isinstance(credentials, dict):
            msg = "Missing credentials."
            raise CredentialError(msg)
        self.credentials = dict(SupermetricsCredentials(**credentials))

        super().__init__(*args, credentials=self.credentials, **kwargs)

        self.query_params = query_params
        self.api_key = self.credentials["api_key"]
        self.user = self.credentials["user"]

    def to_json(self, timeout=(3.05, 60 * 30)) -> Dict[str, Any]:
        """
        Download query results to a dictionary.

        Parameters
        ----------
        timeout : tuple of float, optional
            The timeout values for the request. Defaults to (3.05, 60 * 30) - a short
            timeout for connection issues and a long timeout for query execution.

        Returns
        -------
        dict
            The response from the API as a JSON dictionary.

        Raises
        ------
        ValueError
            If query parameters are not set before calling this method.
        """
        if not self.query_params:
            raise ValueError("Please build the query first")

        params = {"json": json.dumps(self.query_params)}
        headers = {"Authorization": f"Bearer {self.api_key}"}

        response = handle_api_response(
            url=self.API_ENDPOINT, params=params, headers=headers, timeout=timeout
        )
        return response.json()

    @classmethod
    def _get_col_names_google_analytics(
        cls,
        response: dict,
    ) -> List[str]:
        """
        Get column names from Google Analytics data.

        Parameters
        ----------
        response : dict
            Dictionary with the JSON response from the API call.

        Returns
        -------
        list of str
            List of column names for Google Analytics data.

        Raises
        ------
        ValueError
            If no data is returned or if the column names cannot be determined.
        """
        is_pivoted = any(
            field["field_split"] == "column"
            for field in response["meta"]["query"]["fields"]
        )

        if is_pivoted:
            if not response["data"]:
                raise ValueError(
                    "Couldn't find column names as query returned no data."
                )
            columns = response["data"][0]
        else:
            cols_meta = response["meta"]["query"]["fields"]
            columns = [col_meta["field_name"] for col_meta in cols_meta]
        return columns

    @classmethod
    def _get_col_names_other(cls, response: dict) -> List[str]:
        """
        Get column names from non-Google Analytics data.

        Parameters
        ----------
        response : dict
            Dictionary with the JSON response from the API call.

        Returns
        -------
        list of str
            List of column names.
        """
        cols_meta = response["meta"]["query"]["fields"]
        columns = [col_meta["field_name"] for col_meta in cols_meta]
        return columns

    def _get_col_names(self) -> List[str]:
        """
        Get column names based on the data type.

        Returns
        -------
        list of str
            List of column names.

        Raises
        ------
        ValueError
            If the column names cannot be determined.
        """
        response: dict = self.to_json()
        if self.query_params["ds_id"] == "GA":
            return Supermetrics._get_col_names_google_analytics(response)

        return Supermetrics._get_col_names_other(response)

    @add_viadot_metadata_columns
    def to_df(self, if_empty: str = "warn",
              query_params: Dict[str, Any] = None) -> pd.DataFrame:
        """
        Download data into a pandas DataFrame.

        Parameters
        ----------
        if_empty : str, optional
            What to do if the query returns no data. Defaults to "fail".

        Returns
        -------
        pd.DataFrame
            Pandas DataFrame with the JSON data.

        Raises
        ------
        ValueError
            If the DataFrame is empty and `if_empty` is set to "fail".
        """
        # Use provided query_params or default to the instance's query_params
        if query_params is not None:
            self.query_params = query_params

        if not self.query_params:
            raise ValueError("Query parameters are required to fetch data.")

        self.query_params["api_key"] = self.api_key

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
