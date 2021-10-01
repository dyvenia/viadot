from datetime import timedelta
from typing import Any, Dict, List, Union

import pandas as pd
from prefect import Task
from prefect.utilities.tasks import defaults_from_attrs

from ..sources import CloudForCustomers


class CloudForCustomersToCSV(Task):
    """
    Task for downloading data from the Cloud For Customers to a csv file.

    Args:
        if_empty (str, optional): What to do if query returns no data. Defaults to "warn".
    """

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
        params: Dict[str, Any] = {},
    ):
        """
        Run Task CloudForCustomersToCSV.

        Args:
            url (str, optional): The url to the API. Defaults value from credential.json file.
            endpoint (str, optional): The endpoint of the API. Defaults to None.
            params (Dict[str, Any]): The query parameters like filter by creation date time. Defaults to json format.
        """
        cloud_for_customers = CloudForCustomers(
            url=url, endpoint=endpoint, params=params
        )

        # Download data to a local CSV file
        self.logger.info(f"Downloading data to {path}...")
        cloud_for_customers.to_csv(
            path=path, if_exists=if_exists, if_empty=if_empty, sep=sep, fields=fields
        )
        self.logger.info(f"Successfully downloaded data to {path}.")


class CloudForCustomersToDF(Task):
    """
    Task for downloading data from the Cloud For Customers to a pandas DataFrame.

    Args:
        if_empty (str, optional): What to do if query returns no data. Defaults to "warn".
    """

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
        params: Dict[str, Any] = {},
    ):
        """
        Run Task CloudForCustomersToDF.

        Args:
            url (str, optional): The url to the API. Defaults value from credential.json file.
            endpoint (str, optional): The endpoint of the API. Defaults to None.
            params (Dict[str, Any]): The query parameters like filter by creation date time. Defaults to json format.
        """
        cloud_for_customers = CloudForCustomers(
            url=url, endpoint=endpoint, params=params
        )

        df = cloud_for_customers.to_df(if_empty=self.if_empty, fields=fields)
        self.logger.info(f"Successfully downloaded data to a DataFrame.")
        return df
