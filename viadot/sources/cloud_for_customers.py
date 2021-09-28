from .base import Source
import requests
import pandas as pd
from typing import Any, Dict, List
import urllib
from urllib.parse import urljoin
from ..config import local_config


class CloudForCustomers(Source):
    """
    Fetches data from Cloud for Customer.

    Args:
        url (str, optional): The url to the API. Defaults to None.
        endpoint (str, optional): The endpoint of the API. Defaults to None.
        params (Dict[str, Any]): The query parameters like filter by creation date time. Defaults to json format.
    """

    def __init__(
        self,
        *args,
        url: str = None,
        endpoint: str = None,
        params: Dict[str, Any] = {"$format": "json"},
        **kwargs
    ):
        super().__init__(*args, **kwargs)
        credentials = local_config.get("CLOUD_FOR_CUSTOMERS")
        self.API_URL = url
        self.QUERY_ENDPOINT = endpoint
        self.params = params
        self.auth = (credentials["username"], credentials["password"])

    def to_json(self, fields: List[str] = None) -> List:
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
            entity_dict = {}
            ids = 0
            for element in dirty_json["d"]["results"]:
                new_entity = {}
                for key, object_of_interest in element.items():
                    if key != "__metadata" and key in fields:
                        new_entity[key] = object_of_interest
                entity_dict[ids] = new_entity
                ids += 1
            return entity_dict

        except requests.exceptions.HTTPError as e:
            return "Error: " + str(e)

    def to_df(self, fields: List[str] = None, if_empty: str = None) -> pd.DataFrame:
        df = pd.DataFrame([])
        if fields != None:
            entity_dict = self.to_json(fields=fields)
            for k, val in entity_dict.items():
                new_df = pd.DataFrame([val])
                df = df.append(new_df, ignore_index=True)
            return df
        else:
            return df
