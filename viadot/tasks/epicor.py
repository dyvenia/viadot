import xml.etree.ElementTree as ET
from typing import Any, Dict, List, Optional
from xml.etree.ElementTree import fromstring

import pandas as pd
from prefect import Task
from prefect.utilities.tasks import defaults_from_attrs

from ..sources import Epicor


class EpicorOrdersToDF(Task):
    def __init__(
        self,
        base_url: str,
        filters_xml: str,
        credentials: Dict[str, Any] = None,
        config_key: str = None,
        start_date_field: str = "BegInvoiceDate",
        end_date_field: str = "EndInvoiceDate",
        timeout: int = 3600,
        *args,
        **kwargs,
    ) -> pd.DataFrame:
        """
        Task for downloading and parsing orders data from Epicor API to a pandas DataFrame.

        Args:
            name (str): The name of the flow.
            base_url (str, required): Base url to Epicor Orders.
            filters_xml (str, required): Filters in form of XML. The date filter is necessary.
            credentials (Dict[str, Any], optional): Credentials to connect with Epicor Api containing host, port, username and password. Defaults to None.
            config_key (str, optional): Credential key to dictionary where details are stored. Defauls to None.
            start_date_field (str, optional) The name of filters filed containing start date. Defaults to "BegInvoiceDate".
            end_date_field (str, optional) The name of filters filed containing end date. Defaults to "EndInvoiceDate".
            timeout(int, optional): The amount of time (in seconds) to wait while running this task before
                a timeout occurs. Defaults to 3600.

        Returns:
            pd.DataFrame: DataFrame with parsed API output
        """
        self.credentials = credentials
        self.config_key = config_key
        self.base_url = base_url
        self.filters_xml = filters_xml
        self.start_date_field = start_date_field
        self.end_date_field = end_date_field
        super().__init__(
            name="epicor_orders_to_df",
            timeout=timeout,
            *args,
            **kwargs,
        )

    def __call__(self, *args, **kwargs):
        """Load Epicor Orders to DF"""
        return super().__call__(*args, **kwargs)

    @defaults_from_attrs(
        "credentials",
        "config_key",
        "base_url",
        "filters_xml",
        "start_date_field",
        "end_date_field",
    )
    def run(
        self,
        credentials: Dict[str, Any] = None,
        config_key: str = None,
        base_url: str = None,
        filters_xml: str = None,
        start_date_field: str = None,
        end_date_field: str = None,
    ):
        epicor = Epicor(
            credentials=credentials,
            config_key=config_key,
            base_url=base_url,
            filters_xml=filters_xml,
            start_date_field=start_date_field,
            end_date_field=end_date_field,
        )
        return epicor.to_df()
