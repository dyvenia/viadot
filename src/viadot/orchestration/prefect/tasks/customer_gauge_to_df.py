"""
'customer_gauge_to_df.py'.

Prefect task wrapper for the Customer Gauge Cloud API connector.

This module provides an intermediate wrapper between the prefect flow and the connector:
- Generate the Customer Gauge Cloud API connector.
- Create and return a pandas Data Frame with the response of the API.

Functions:

    customer_gauge_to_df(
        endpoint=endpoint,
        endpoint_url=endpoint_url,
        total_load=total_load,
        cursor=cursor,
        pagesize=pagesize,
        date_field=date_field,
        start_date=start_date,
        end_date=end_date,
        unpack_by_field_reference_cols=unpack_by_field_reference_cols,
        unpack_by_nested_dict_transformer=unpack_by_nested_dict_transformer,
        credentials=credentials,
        customer_gauge_credentials_secret=customer_gauge_credentials_secret,
        anonymize=anonymize,
        columns_to_anonymize=columns_to_anonymize,
        anonymize_method=anonymize_method,
        anonymize_value=anonymize_value,
        date_column=date_column,
        days=days,
        validate_df_dict=validate_df_dict,
        timeout=timeout,
        *args,
        **kwargs,
    ):
        Task to download data from Customer Gauge Cloud API.
"""  # noqa: D412

from datetime import datetime
from typing import Any, Dict, List, Literal

import pandas as pd
from prefect import get_run_logger, task

from viadot.orchestration.prefect.exceptions import MissingSourceCredentialsError
from viadot.orchestration.prefect.utils import get_credentials
from viadot.sources import CustomerGauge


@task(retries=3, log_prints=True, retry_delay_seconds=10, timeout_seconds=2 * 60 * 60)
def customer_gauge_to_df(
    *args,
    endpoint: Literal["responses", "non-responses"] = None,
    endpoint_url: str = None,
    total_load: bool = True,
    cursor: int = None,
    pagesize: int = 1000,
    date_field: Literal[
        "date_creation", "date_order", "date_sent", "date_survey_response"
    ] = None,
    start_date: datetime = None,
    end_date: datetime = None,
    unpack_by_field_reference_cols: List[str] = None,
    unpack_by_nested_dict_transformer: List[str] = None,
    credentials: Dict[str, Any] = None,
    customer_gauge_credentials_secret: str = "CUSTOMER-GAUGE",
    anonymize: bool = False,
    columns_to_anonymize: List[str] = None,
    anonymize_method: Literal["mask", "hash"] = "mask",
    anonymize_value: str = "***",
    date_column: str = None,
    days: int = None,
    validate_df_dict: dict = None,
    timeout: int = 3600,
    **kwargs,
) -> pd.DataFrame:
    """
    Task customer_gauge_to_df for downloading the selected range of data.

    From Customer Gauge endpoint and return as one pandas DataFrame.

    Args:
        endpoint (Literal["responses", "non-responses"], optional): Indicate
            which endpoint to connect. Defaults to None.
        endpoint_url (str, optional): Full URL for pointing to specific
            endpoint. Defaults to None.
        total_load (bool, optional): Indicate whether to download the data to
            the latest. If 'False', only one API call is executed (up to 1000
            records). Defaults to True.
        cursor (int, optional): Cursor value to navigate to the page. Defaults
            to None.
        pagesize (int, optional): Number of responses (records) returned per
            page, max value = 1000. Defaults to 1000.
        date_field (Literal["date_creation", "date_order", "date_sent",
            "date_survey_response"], optional): Specifies the date type which
            filter date range. Defaults to None.
        start_date (datetime, optional): Defines the period start date in
            yyyy-mm-dd format. Defaults to None.
        end_date (datetime, optional): Defines the period end date in
            yyyy-mm-dd format. Defaults to None.
        unpack_by_field_reference_cols (List[str], optional): Columns to unpack
            and modify using `_field_reference_unpacker`. Defaults to None.
        unpack_by_nested_dict_transformer (List[str], optional): Columns to
            unpack and modify using `_nested_dict_transformer`. Defaults to
            None.
        credentials (Dict[str, Any], optional): Credentials to connect with API
            containing client_id, client_secret. Defaults to None.
        customer_gauge_credentials_secret (str, optional): The name of the
            Azure Key Vault secret containing a dictionary with ['client_id',
            'client_secret']. Defaults to "CUSTOMER-GAUGE".
        anonymize (bool, optional): Indicates if anonymize selected columns.
            Defaults to False.
        columns_to_anonymize (List[str], optional): List of columns to anonymize.
            Defaults to None.
        anonymize_method  (Literal["mask", "hash"], optional): Method of
            anonymizing data. "mask" -> replace the data with "value" arg. "hash" ->
            replace the data with the hash value of an object (using `hash()` method).
            Defaults to "mask".
        anonymize_value (str, optional): Value to replace the data. Defaults to "***".
        date_column (str, optional): Name of the date column used to identify rows
            that are older than a specified number of days. Defaults to None.
        days (int, optional): The number of days beyond which we want to anonymize
            the data, e.g. older than 2 years can be: 2*365. Defaults to None.
        validate_df_dict (Dict[str], optional): A dictionary with optional list of tests
            to verify the output dataframe. If defined, triggers the `validate_df`
            task from task_utils. Defaults to None.
        timeout (int, optional): The time (in seconds) to wait while running
            this task before a timeout occurs. Defaults to 3600.
    """
    logger = get_run_logger()

    if credentials is None:
        credentials = get_credentials(customer_gauge_credentials_secret)
        if credentials is None:
            raise MissingSourceCredentialsError

    customer_gauge = CustomerGauge(
        *args,
        endpoint=endpoint,
        endpoint_url=endpoint_url,
        total_load=total_load,
        cursor=cursor,
        pagesize=pagesize,
        date_field=date_field,
        start_date=start_date,
        end_date=end_date,
        unpack_by_field_reference_cols=unpack_by_field_reference_cols,
        unpack_by_nested_dict_transformer=unpack_by_nested_dict_transformer,
        credentials=credentials,
        anonymize=anonymize,
        columns_to_anonymize=columns_to_anonymize,
        anonymize_method=anonymize_method,
        anonymize_value=anonymize_value,
        date_column=date_column,
        days=days,
        validate_df_dict=validate_df_dict,
        timeout=timeout,
        **kwargs,
    )

    logger.info("running `to_df` method:\n")
    data_frame = customer_gauge.to_df()

    return data_frame
