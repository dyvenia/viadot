import json
import pandas as pd

from typing import List, Dict, Any
from prefect import Task
from prefect.tasks.secrets import PrefectSecret
from prefect.utilities import logging
from viadot.sources import Hubspot

logger = logging.get_logger()


class HubspotToDF(Task):
    def __init__(
        self,
        *args,
        **kwargs,
    ):

        super().__init__(
            name="hubspot_to_df",
            *args,
            **kwargs,
        )

    def __call__(self):
        """Download Hubspot data to a DF"""
        super().__call__(self)

    def to_df(self, result: list = None):
        return pd.json_normalize(result)

    def run(
        self,
        endpoint: str,
        properties: List[Any] = None,
        filters: Dict[str, Any] = {},
        nrows: int = 1000,
    ) -> pd.DataFrame:

        hubspot = Hubspot()

        query = dict(
            endpoint=endpoint, properties=properties, filters=filters, nrows=nrows
        )

        url = hubspot.get_single_property_url(endpoint=endpoint, properties=properties)
        body = hubspot.get_api_body(filters=filters)

        partition = hubspot.to_json(url=url, body=body)
        full_dataset = partition["results"]

        while "paging" in partition.keys() and len(full_dataset) < nrows:
            body = json.loads(hubspot.get_api_body(filters=filters))
            body["after"] = partition["paging"]["next"]["after"]
            partition = hubspot.to_json(url=url, body=json.dumps(body))
            full_dataset.extend(partition["results"])

        df = self.to_df(full_dataset)[:nrows]

        return df
