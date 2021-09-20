from .base import Source
import pyodata
import requests
import pandas as pd
from typing import Any, Dict, List
from pyodata.v2.service import EntityContainer, EntityProxy


class EntityContainerByKey(EntityContainer):
    """
    This class is overriding of pyodata class
    It allows for EntityContainer['Employee'] instead of EntityContainer.Employee
    """

    def __getitem__(self, name):
        return getattr(self, name)


class EntityProxyByKey(EntityProxy):
    """
    This class is overriding of pyodata class
    It allows for EntityProxy['Employee'] instead of EntityContainer.Employee
    """

    def __getitem__(self, name):
        return getattr(self, name)


class CloudForCustomers(Source):
    """
    Fetches data from Cloud for Customer.

    Parameters
    ----------
    api_url : str, optional
        The URL endpoint to call, by default northwind test API
    """

    def __init__(
        self,
        *args,
        url: str = "http://services.odata.org/V2/Northwind/Northwind.svc/",
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.SERVICE_URL = url
        self.source = pyodata.Client(self.SERVICE_URL, requests.Session())

    def to_df(
        self,
        entity_name: str = None,
        fields: List[str] = None,
        if_empty: str = None,
    ) -> pd.DataFrame:

        if fields is not None and entity_name is not None:
            df = pd.DataFrame(columns=fields)
            entity = self.source.entity_sets
            entity.__class__ = EntityContainerByKey
            entity = self.source.entity_sets[entity_name]
            entity_list = entity.get_entities()

            for entity in entity_list.execute():
                entity.__class__ = EntityProxyByKey
                mini_dict = {}
                for index, field in enumerate(fields):
                    mini_dict[field] = entity[field]
                df = df.append(mini_dict, ignore_index=True)
            return df
        else:
            return pd.DataFrame([])

    def to_json(self, entity_name: str = None, fields: List[str] = None):

        url = f"{self.SERVICE_URL}{entity_name}?$format=json"
        headers = {"Accept": "application/json"}
        try:
            response = requests.get(url, params={}, headers=headers)
            dirty_json = response.json()
            clean_json = {}
            for element in dirty_json["d"]["results"]:
                for key, object_of_interest in element.items():
                    if key != "__metadata" and key != "Employee" and key in fields:
                        clean_json[key] = object_of_interest
            return clean_json
        except requests.exceptions.HTTPError as e:
            return "Error: " + str(e)
