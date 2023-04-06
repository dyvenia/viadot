from prefect.utilities import logging
from prefect import Task
from prefect.utilities.tasks import defaults_from_attrs
from typing import Literal

import pandas as pd
import json
from datetime import datetime

from viadot.tasks.azure_key_vault import AzureKeyVaultSecret
from viadot.sources.customer_gauge import CustomerGauge

logger = logging.get_logger()


class CustomerGaugeToDF(Task):
    def __init__(
        self,
        endpoint: Literal["responses", "non-responses"] = None,
        total_load: bool = True,
        cursor: int = None,
        pagesize: int = 1000,
        date_field: Literal[
            "date_creation", "date_order", "date_sent", "date_survey_response"
        ] = None,
        start_date: datetime = None,
        end_date: datetime = None,
        timeout: int = 3600,
        *args,
        **kwargs,
    ):
        """
        Task CustomerGaugeToDF for downloading the selected range of data from Customer Gauge endpoint and return as one pandas DataFrame.

        Args:
            endpoint (Literal["responses", "non-responses"], optional): Indicate which endpoint to connect. Defaults to None.
            total_load (bool, optional): Indicate whether to download the data to the latest. If 'False', only one API call is executed (up to 1000 records). Defaults to True.
            cursor (int, optional): Cursor value to navigate to the page. Defaults to None.
            pagesize (int, optional): Number of responses (records) returned per page, max value = 1000. Defaults to 1000.
            date_field (Literal["date_creation", "date_order", "date_sent", "date_survey_response"], optional): Specifies the date type which filter date range. Defaults to None.
            start_date (datetime, optional): Defines the period end date in yyyy-mm-dd format. Defaults to None.
            end_date (datetime, optional): Defines the period start date in yyyy-mm-dd format. Defaults to None.
            timeout (int, optional): The time (in seconds) to wait while running this task before a timeout occurs. Defaults to 3600.
        """
        self.endpoint = endpoint
        self.total_load = total_load
        self.cursor = cursor
        self.pagesize = pagesize
        self.date_field = date_field
        self.start_date = start_date
        self.end_date = end_date

        super().__init__(
            name="customer_gauge_to_df",
            timeout=timeout,
            *args,
            **kwargs,
        )

    def __call__(self):
        """Download Customer Gauge data to a DF"""
        super().__call__(self)

    @defaults_from_attrs(
        "endpoint",
        "total_load",
        "cursor",
        "pagesize",
        "date_field",
        "start_date",
        "end_date",
    )
    def run(
        self,
        endpoint: Literal["responses", "non-responses"] = None,
        total_load: bool = True,
        cursor: int = None,
        pagesize: int = 1000,
        date_field: Literal[
            "date_creation", "date_order", "date_sent", "date_survey_response"
        ] = None,
        start_date: datetime = None,
        end_date: datetime = None,
        credentials_secret: str = "CUSTOMER-GAUGE",
        vault_name: str = None,
    ) -> pd.DataFrame:
        """
        Run method. Downloading the selected range of data from Customer Gauge endpoint and return as one pandas DataFrame.

        Args:
            endpoint (Literal["responses", "non-responses"]): Indicate which endpoint to connect. Defaults to None.
            total_load (bool, optional): Indicate whether to download the data to the latest. If 'False', only one API call is executed (up to 1000 records). Defaults to True.
            cursor (int, optional): Cursor value to navigate to the page. Defaults to None.
            pagesize (int, optional): Number of responses (records) returned per page, max value = 1000. Defaults to 1000.
            date_field (Literal["date_creation", "date_order", "date_sent", "date_survey_response"], optional): Specifies the date type which filter date range. Defaults to None.
            start_date (datetime, optional): Defines the period end date in yyyy-mm-dd format. Defaults to None.
            end_date (datetime, optional): Defines the period start date in yyyy-mm-dd format. Defaults to None.
            credentials_secret (str, optional): The name of the Azure Key Vault secret containing a dictionary with ['client_id', 'client_secret']. Defaults to "CUSTOMER-GAUGE".
            vault_name (str, optional): The name of the vault from which to obtain the secret. Defaults to None.

        Returns:
            pd.DataFrame: Final pandas DataFrame.
        """
        try:
            credentials_str = AzureKeyVaultSecret(
                credentials_secret, vault_name=vault_name
            ).run()
        except ValueError:
            pass

        credentials = json.loads(credentials_str)

        df_list = []

        customer_gauge = CustomerGauge(endpoint=endpoint, credentials=credentials)
        logger.info(f"Starting downloading data from {self.endpoint} endpoint...")
        json_data = customer_gauge.get_json_response(
            cursor=cursor,
            pagesize=pagesize,
            date_field=date_field,
            start_date=start_date,
            end_date=end_date,
        )
        cur = customer_gauge.get_cursor(json_data)
        df = customer_gauge.to_df(json_data)
        df_list.append(df)
        if total_load == True:
            if cursor is None:
                logger.info(
                    f"Downloading all the data from the {self.endpoint} endpoint. Process might take a few minutes..."
                )
            else:
                logger.info(
                    f"Downloading starting from the {cursor} cursor. Process might take a few minutes..."
                )
            while df.empty == False:
                json_data = customer_gauge.get_json_response(cursor=cur)
                cur = customer_gauge.get_cursor(json_data)
                df = customer_gauge.to_df(json_data)
                df_list.append(df)

        df_total = pd.concat(df_list, ignore_index=True)
        logger.info(f"Downloading finished.")

        return df_total
