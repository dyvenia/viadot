import json
import time
import urllib
from typing import Any, Dict

import pandas as pd
import requests
from prefect.utilities import logging

from ..config import local_config
from .base import Source

logger = logging.get_logger(__name__)


class Supermetrics(Source):
    """
    A class implementing the Supermetrics API.

    Documentation for this API is located at: https://supermetrics.com/docs/product-api-getting-started/

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
        response = requests.get(self.API_ENDPOINT, params=params)
        assert response.ok

        response_json = response.json()

        return response_json

    def to_df(self) -> pd.DataFrame:
        data = self.to_json()["data"]
        df = pd.DataFrame(data[1:], columns=data[0])
        return df

    def query(self, params: Dict[str, Any]):
        self.query_params = params
        self.query_params["api_key"] = self.credentials["API_KEY"]
        return self
