"""'customer_gauge_to_adls.py'."""

from datetime import datetime
from typing import Any, Literal

from prefect import flow

from viadot.orchestration.prefect.tasks import customer_gauge_to_df, df_to_adls


@flow
def customer_gauge_to_adls(  # noqa: PLR0913
    config_key: str | None = None,
    azure_key_vault_secret: str | None = None,
    endpoint: Literal["responses", "non-responses"] = "non-responses",
    cursor: int | None = None,
    pagesize: int = 1000,
    date_field: str | None = None,
    start_date: datetime | None = None,
    end_date: datetime | None = None,
    total_load: bool = True,
    unpack_by_field_reference_cols: list[str] | None = None,
    unpack_by_nested_dict_transformer: list[str] | None = None,
    validate_df_dict: dict[str, Any] | None = None,
    anonymize: bool = False,
    columns_to_anonymize: list[str] | None = None,
    anonymize_method: Literal["mask", "hash"] = "mask",
    anonymize_value: str = "***",
    date_column: str | None = None,
    days: int | None = None,
    adls_azure_key_vault_secret: str | None = None,
    adls_config_key: str | None = None,
    adls_path: str | None = None,
    adls_path_overwrite: bool = False,
) -> None:
    """Download data from the Customer Gauge API using Prefect.

    Args:
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
        date_field (str, optional): Specifies the date type which filter date range.
            Possible options: "date_creation", "date_order", "date_sent" or
            "date_survey_response". Defaults to None.
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
        anonymize (bool, optional): Indicates if anonymize selected columns.
            Defaults to False.
        columns_to_anonymize (list[str], optional): List of columns to anonymize.
            Defaults to None.
        anonymize_method  (Literal["mask", "hash"], optional): Method of
            anonymizing data. "mask" -> replace the data with "value" arg. "hash" ->
            replace the data with the hash value of an object (using `hash()`
            method). Defaults to "mask".
        anonymize_value (str, optional): Value to replace the data.
            Defaults to "***".
        date_column (str, optional): Name of the date column used to identify rows
            that are older than a specified number of days. Defaults to None.
        days (int, optional): The number of days beyond which we want to anonymize
            the data, e.g. older than 2 years can be: 2*365. Defaults to None.
        adls_azure_key_vault_secret (str, optional): The name of the Azure Key.
            Defaults to None.
        adls_config_key (str, optional): The key in the viadot config holding relevant
            credentials. Defaults to None.
        adls_path (str, optional): Azure Data Lake destination folder/catalog path.
            Defaults to None.
        adls_path_overwrite (bool, optional): Whether to overwrite the file in ADLS.
            Defaults to False.
    """
    data_frame = customer_gauge_to_df(
        config_key=config_key,
        azure_key_vault_secret=azure_key_vault_secret,
        endpoint=endpoint,
        cursor=cursor,
        pagesize=pagesize,
        date_field=date_field,
        start_date=start_date,
        end_date=end_date,
        total_load=total_load,
        unpack_by_field_reference_cols=unpack_by_field_reference_cols,
        unpack_by_nested_dict_transformer=unpack_by_nested_dict_transformer,
        validate_df_dict=validate_df_dict,
        anonymize=anonymize,
        columns_to_anonymize=columns_to_anonymize,
        anonymize_method=anonymize_method,
        anonymize_value=anonymize_value,
        date_column=date_column,
        days=days,
    )

    return df_to_adls(
        df=data_frame,
        path=adls_path,
        credentials_secret=adls_azure_key_vault_secret,
        config_key=adls_config_key,
        overwrite=adls_path_overwrite,
    )
