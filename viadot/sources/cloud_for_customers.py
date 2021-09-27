from .base import Source
import requests
import pandas as pd
from typing import Any, Dict, List
import urllib
from urllib.parse import urljoin


class CloudForCustomers(Source):
    """
    Fetches data from Cloud for Customer.

    Parameters
    ----------
    api_url : str, optional
        The URL endpoint to call, by default northwind test API
    endpoint : str, optional
    params : Dict[str, Any] optional,
             like filter parameters
    """

    def __init__(
        self,
        *args,
        url: str = None,
        endpoint: str = None,
        username: str = None,
        password: str = None,
        params: Dict[str, Any] = {"$format": "json"},
        **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.API_URL = url
        self.QUERY_ENDPOINT = endpoint
        self.params = params
        self.auth = (username, password)

    def to_json(self, fields: List[str] = None):
        try:
            for key, val in self.params.items():
                if key != "$format":
                    requests.utils.quote(self.params.get(key))
            response = requests.get(
                urljoin(self.API_URL, self.QUERY_ENDPOINT),
                params=self.params,
                auth=self.auth,
            )
            dirty_json = response.json()
            clean_json = {}
            for element in dirty_json["d"]["results"]:
                for key, object_of_interest in element.items():
                    if key != "__metadata" and key in fields:
                        clean_json[key] = object_of_interest
            return clean_json
        except requests.exceptions.HTTPError as e:
            return "Error: " + str(e)

    def to_df(self, fields: List[str] = None, if_empty: str = None) -> pd.DataFrame:
        if fields != None:
            data = self.to_json(fields=fields)
            df = pd.DataFrame([data])
            return df
        else:
            return pd.DataFrame([])

    def to_df(self, fields: List[str] = None, if_empty: str = None) -> pd.DataFrame:
        if fields is not None:
            data = self.to_json(fields=fields)
            df = pd.DataFrame([data])
            return df
        else:
            return pd.DataFrame([])
