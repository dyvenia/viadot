"""'customer_gauge_to_df.py'."""

from datetime import datetime
from typing import Any, Literal

import pandas as pd
from prefect import task

from viadot.orchestration.prefect.exceptions import MissingSourceCredentialsError
from viadot.orchestration.prefect.utils import get_credentials
from viadot.sources import CustomerGauge


@task(retries=3, log_prints=True, retry_delay_seconds=10, timeout_seconds=2 * 60 * 60)
def customer_gauge_to_df(
    credentials: dict[str, Any] | None = None,
    config_key: str | None = None,
    azure_key_vault_secret: str | None = None,
    endpoint: Literal["responses", "non-responses"] = "non-responses",
    cursor: int | None = None,
    pagesize: int = 1000,
    date_field: Literal[
        "date_creation", "date_order", "date_sent", "date_survey_response"
    ] = "date_creation",
    start_date: datetime | None = None,
    end_date: datetime | None = None,
    total_load: bool = True,
    unpack_by_field_reference_cols: list[str] | None = None,
    unpack_by_nested_dict_transformer: list[str] | None = None,
    validate_df_dict: dict[str] | None = None,
) -> pd.DataFrame:
    """Download the selected range of data from Customer Gauge API.

    Args:
        credentials (dict[str, Any], optional): Credentials to connect with API
            containing client_id, client_secret. Defaults to None.
        config_key (str, optional): The key in the viadot config holding relevant
            credentials. Defaults to None.
        azure_key_vault_secret (str, optional): The name of the Azure Key Vault secret
            containing a dictionary with ['client_id', 'client_secret'].
            Defaults to None.
        endpoint (Literal["responses", "non-responses"], optional): Indicate which
            endpoint to connect. Defaults to "non-responses.
        cursor (int, optional): Cursor value to navigate to the page.
            Defaults to None.
        pagesize (int, optional): Number of responses (records) returned per page,
            max value = 1000. Defaults to 1000. Defaults to 1000.
        date_field (Literal["date_creation", "date_order", "date_sent",
            "date_survey_response"], optional): Specifies the date type which
            filter date range. Defaults to "date_creation.
        start_date (datetime, optional): Defines the period start date in
            yyyy-mm-dd format. Defaults to None.
        end_date (datetime, optional): Defines the period end date in
            yyyy-mm-dd format. Defaults to None.
        total_load (bool, optional): Indicate whether to download the data to the
            latest. If 'False', only one API call is executed (up to 1000 records).
            Defaults to True.
        unpack_by_field_reference_cols (list[str]): Columns to unpack and modify using
            `_field_reference_unpacker`. Defaults to None.
        unpack_by_nested_dict_transformer (list[str]): Columns to unpack and modify
            using `_nested_dict_transformer`. Defaults to None.
        validate_df_dict (dict[str], optional): A dictionary with optional list of
            tests to verify the output dataframe. If defined, triggers the
            `validate_df` task from task_utils. Defaults to None.

    Raises:
        MissingSourceCredentialsError: If none credentials have been provided.

    Returns:
        pd.DataFrame: The response data as a Pandas Data Frame.
    """
    if not (azure_key_vault_secret or config_key or credentials):
        raise MissingSourceCredentialsError

    if not config_key:
        credentials = credentials or get_credentials(azure_key_vault_secret)

    customer_gauge = CustomerGauge(credentials=credentials, config_key=config_key)
    customer_gauge.api_connection(
        endpoint=endpoint,
        cursor=cursor,
        pagesize=pagesize,
        date_field=date_field,
        start_date=start_date,
        end_date=end_date,
        total_load=total_load,
        unpack_by_field_reference_cols=unpack_by_field_reference_cols,
        unpack_by_nested_dict_transformer=unpack_by_nested_dict_transformer,
    )

    return customer_gauge.to_df(validate_df_dict=validate_df_dict)
