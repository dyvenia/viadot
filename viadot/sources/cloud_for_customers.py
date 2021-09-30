from .base import Source
import requests
import pandas as pd
from typing import Any, Dict, List
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
        self.api_url = url or credentials["server"]
        self.query_endpoint = endpoint
        self.params = params
        self.auth = (credentials["username"], credentials["password"])

    def to_records(self) -> List:
        try:
            response = requests.get(
                urljoin(self.api_url, self.query_endpoint),
                params=self.params,
                auth=self.auth,
            )
            response.raise_for_status()

        except Exception as e:
            raise

        dirty_json = response.json()
        entity_list = []
        for element in dirty_json["d"]["results"]:
            new_entity = {}
            for key, object_of_interest in element.items():
                if key != "__metadata" and key != "Photo" and key != "":
                    if "{" not in str(object_of_interest):
                        new_entity[key] = object_of_interest
            entity_list.append(new_entity)
        return entity_list

    def to_df(self, fields: List[str] = None, if_empty: str = None) -> pd.DataFrame:
        records = self.to_records()
        df = pd.DataFrame(data=records)
        if fields:
            return df[fields]
        return df
