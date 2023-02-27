import json
import urllib
from copy import deepcopy
from typing import Any, Dict, List

import numpy as np
import pandas as pd
from prefect.utilities import logging

from ..config import local_config
from ..exceptions import CredentialError
from ..utils import handle_api_response
from .base import Source

logger = logging.get_logger(__name__)


class Supermetrics(Source):
    """
    A class implementing the Supermetrics API.

    Documentation for this API is located at: https://supermetrics.com/docs/product-api-getting-started/
    Usage limits: https://supermetrics.com/docs/product-api-usage-limits/

    Parameters
    ----------
    query_params : Dict[str, Any], optional
        The parameters to pass to the GET query.
        See https://supermetrics.com/docs/product-api-get-data/ for full specification,
        by default None
    """

    API_ENDPOINT = "https://api.supermetrics.com/enterprise/v2/query/data/json"

    def __init__(self, *args, query_params: Dict[str, Any] = None, **kwargs):
        DEFAULT_CREDENTIALS = local_config.get("SUPERMETRICS")
        credentials = kwargs.pop("credentials", DEFAULT_CREDENTIALS)
        if credentials is None:
            raise CredentialError("Missing credentials.")
        super().__init__(*args, credentials=credentials, **kwargs)
        self.query_params = query_params

    @classmethod
    def get_params_from_api_query(cls, url: str) -> Dict[str, Any]:
        """Returns parmeters from API query in a dictionary"""
        url_unquoted = urllib.parse.unquote(url)
        s = urllib.parse.parse_qs(url_unquoted)
        endpoint = list(s.keys())[0]
        params = s[endpoint][0]
        params_d = json.loads(params)
        return params_d

    @classmethod
    def from_url(cls, url: str, credentials: Dict[str, Any] = None):
        obj = Supermetrics(credentials=credentials or local_config.get("SUPERMETRICS"))
        params = cls.get_params_from_api_query(url)
        obj.query_params = params
        return obj

    def to_json(self, timeout=(3.05, 60 * 30)) -> Dict[str, Any]:
        """Download query results to a dictionary.
        Note that Supermetrics API will sometimes hang and not return any error message,
        so we're adding a timeout to GET.

        See [requests docs](https://docs.python-requests.org/en/master/user/advanced/#timeouts)
        for an explanation of why this timeout value will work on long-running queries but fail fast
        on connection issues.
        """

        if not self.query_params:
            raise ValueError("Please build the query first")

        params = {"json": json.dumps(self.query_params)}
        headers = {"Authorization": f'Bearer {self.credentials["API_KEY"]}'}

        response = handle_api_response(
            url=self.API_ENDPOINT, params=params, headers=headers, timeout=timeout
        )
        return response.json()

    @classmethod
    def _get_col_names_google_analytics(
        cls,
        response: dict,
    ) -> List[str]:
        """Returns list of Google Analytics columns names"""

        # Supermetrics allows pivoting GA data, in which case it generates additional columns,
        # which are not enlisted in response's query metadata but are instead added as the first row of data.
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
            # non-pivoted data; query fields match result fields
            cols_meta = response["meta"]["query"]["fields"]
            columns = [col_meta["field_name"] for col_meta in cols_meta]
        return columns

    @classmethod
    def _get_col_names_other(cls, response: dict) -> List[str]:
        """Returns list of columns names (to Google Analytics use  _get_col_names_google_analytics ()"""
        cols_meta = response["meta"]["query"]["fields"]
        columns = [col_meta["field_name"] for col_meta in cols_meta]
        return columns

    def _get_col_names(self) -> List[str]:
        """Returns list of columns names"""

        query_params_cp = deepcopy(self.query_params)
        query_params_cp["offset_start"] = 0
        query_params_cp["offset_end"] = 0
        response: dict = Supermetrics(query_params=query_params_cp).to_json()
        if self.query_params["ds_id"] == "GA":
            return Supermetrics._get_col_names_google_analytics(response)
        else:
            return Supermetrics._get_col_names_other(response)

    def to_df(self, if_empty: str = "warn") -> pd.DataFrame:
        """Download data into a pandas DataFrame.

        Note that Supermetric can calculate some fields on the fly and alias them in the
        returned result. For example, if the query requests the `position` field,
        Supermetric may return an `Average position` caclulated field.
        For this reason we take columns names from the actual results rather than from input fields.

        Args:
            if_empty (str, optional): What to do if query returned no data. Defaults to "warn".

        Returns:
            pd.DataFrame: the DataFrame containing query results
        """
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

    def query(self, params: Dict[str, Any]):
        self.query_params = params
        self.query_params["api_key"] = self.credentials["API_KEY"]
        return self
