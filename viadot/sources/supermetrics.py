import json
import urllib
from copy import deepcopy
from typing import Any, Dict, List

import numpy as np
import pandas as pd
import requests
from prefect.utilities import logging
from requests.adapters import HTTPAdapter
from requests.exceptions import ConnectionError, HTTPError, ReadTimeout, Timeout
from requests.packages.urllib3.util.retry import Retry
from urllib3.exceptions import ProtocolError

from ..config import local_config
from .base import Source

logger = logging.get_logger(__name__)


class APIError(Exception):
    pass


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
        super().__init__(*args, credentials=credentials, **kwargs)
        self.query_params = query_params

    @classmethod
    def get_params_from_api_query(cls, url: str) -> Dict[str, Any]:
        url_unquoted = urllib.parse.unquote(url)
        s = urllib.parse.parse_qs(url_unquoted)
        params = s[
            "https://api.supermetrics.com/enterprise/v2/query/data/powerbi?json"
        ][0]
        params_d = json.loads(params)
        return params_d

    @classmethod
    def from_url(cls, url: str, credentials: Dict[str, Any] = None):
        obj = Supermetrics(credentials=credentials)
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

        try:
            session = requests.Session()
            retry_strategy = Retry(
                total=3, status_forcelist=[429, 500, 502, 503, 504], backoff_factor=1
            )
            adapter = HTTPAdapter(max_retries=retry_strategy)

            session.mount("http://", adapter)
            session.mount("https://", adapter)

            response = session.get(
                self.API_ENDPOINT, params=params, headers=headers, timeout=timeout
            )
            response.raise_for_status()
        except ReadTimeout as e:
            msg = "The connection was successful, "
            msg += f"however the API call to {self.API_ENDPOINT} timed out after {timeout[1]}s "
            msg += "while waiting for the server to return data."
            raise APIError(msg)
        except HTTPError as e:
            raise APIError(
                f"The API call to {self.API_ENDPOINT} failed. "
                "Perhaps your account credentials need to be refreshed?",
            ) from e
        except (ConnectionError, Timeout) as e:
            raise APIError(
                f"The API call to {self.API_ENDPOINT} failed due to connection issues."
            ) from e
        except ProtocolError as e:
            raise APIError(
                f"Did not receive any reponse for the API call to {self.API_ENDPOINT}."
            )
        except Exception as e:
            raise APIError("Unknown error.") from e

        return response.json()

    @staticmethod
    def __get_col_names_google_analytics(response: dict) -> List[str]:
        """This is required as Supermetrics allows pivoting GA data but does not return
        the pivoted table's column names in the meta field. Due to this, we're
        forced to read them from the data."""
        try:
            return response["data"][0]
        except IndexError as e:
            raise ValueError(
                "Couldn't find column names as query returned no data"
            ) from e

    @staticmethod
    def __get_col_names_other(response: dict) -> List[str]:
        cols_meta = response["meta"]["query"]["fields"]
        columns = [col_meta["field_name"] for col_meta in cols_meta]
        return columns

    def _get_col_names(self) -> List[str]:

        query_params_cp = deepcopy(self.query_params)
        query_params_cp["offset_start"] = 0
        query_params_cp["offset_end"] = 0
        response: dict = Supermetrics(query_params=query_params_cp).to_json()
        if self.query_params["ds_id"] == "GA":
            return self.__get_col_names_google_analytics(response)
        else:
            return self.__get_col_names_other(response)

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
        columns = self._get_col_names()
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
