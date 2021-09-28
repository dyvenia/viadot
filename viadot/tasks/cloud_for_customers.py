from datetime import timedelta
from typing import Any, Dict, List, Union

import pandas as pd
from prefect import Task
from prefect.utilities.tasks import defaults_from_attrs

from ..sources import CloudForCustomers


class CloudForCustomersToCSV(Task):
    def __init__(
        self,
        *args,
        path: str = "cloud_for_customers_extract.csv",
        if_exists: str = "replace",
        if_empty: str = "warn",
        sep="\t",
        **kwargs,
    ):

        self.path = path
        self.if_exists = if_exists
        self.if_empty = if_empty
        self.sep = sep

        super().__init__(
            name="cloud_for_customers_to_csv",
            *args,
            **kwargs,
        )

    def __call__(self):
        """Download Cloud For Customers data to a CSV"""
        super().__call__(self)

    @defaults_from_attrs(
        "path",
        "if_exists",
        "if_empty",
        "sep",
    )
    def run(
        self,
        path: str = None,
        url: str = None,
        endpoint: str = None,
        fields: List[str] = None,
        if_exists: str = None,
        if_empty: str = None,
        sep: str = None,
    ):

        cloud_for_customers = CloudForCustomers(url=url, endpoint=endpoint)

        # Download data to a local CSV file
        self.logger.info(f"Downloading data to {path}...")
        cloud_for_customers.to_csv(
            path=path, if_exists=if_exists, if_empty=if_empty, sep=sep, fields=fields
        )
        self.logger.info(f"Successfully downloaded data to {path}.")


class CloudForCustomersToDF(Task):
    def __init__(
        self,
        *args,
        if_empty: str = "warn",
        **kwargs,
    ):

        self.if_empty = if_empty

        super().__init__(
            name="cloud_for_customers_to_df",
            *args,
            **kwargs,
        )

    def __call__(self):
        """Download Cloud For Customers data to a DF"""
        super().__call__(self)

    def run(
        self,
        url: str = None,
        endpoint: str = None,
        fields: List[str] = None,
    ):

        cloud_for_customers = CloudForCustomers(url=url, endpoint=endpoint)

        df = cloud_for_customers.to_df(if_empty=self.if_empty, fields=fields)
        self.logger.info(f"Successfully downloaded data to a DataFrame.")
        return df
