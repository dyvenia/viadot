import json
import urllib
from copy import deepcopy
from typing import Any, Dict

import pandas as pd
import requests
from prefect.utilities import logging
from requests.exceptions import ConnectionError, HTTPError, Timeout

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
        obj.query_params["api_key"] = credentials["API_KEY"]
        return obj

    def to_json(self) -> Dict[str, Any]:

        if not self.query_params:
            raise ValueError("Please build the query first")

        self.query_params["api_key"] = self.credentials["API_KEY"]
        params = {"json": json.dumps(self.query_params)}

        try:
            response = requests.get(self.API_ENDPOINT, params=params)
            response.raise_for_status()
        except (HTTPError, ConnectionError, Timeout) as e:
            raise APIError(f"The API call to {self.API_ENDPOINT} failed.") from e

        return response.json()

    def to_df(self) -> pd.DataFrame:
        """Download data into a pandas DataFrame.

        Note that Supermetric can calculate some fields on the fly and alias them in the
        returned result. For example, if the query requests the `position` field,
        Supermetric may return an `Average position` caclulated field.
        For this reason we take columns names from the actual results rather than from input fields.

        Returns:
            pd.DataFrame: the DataFrame containing query results
        """
        data = self.to_json()["data"]
        if data:
            df = pd.DataFrame(data[1:], columns=data[0])
        else:
            query_params_cp = deepcopy(self.query_params)
            query_params_cp["offset_start"] = 0
            query_params_cp["offset_end"] = 0
            columns = Supermetrics(query_params=query_params_cp).to_df().columns
            df = pd.DataFrame(columns=columns)
        return df

    def query(self, params: Dict[str, Any]):
        self.query_params = params
        self.query_params["api_key"] = self.credentials["API_KEY"]
        return self
