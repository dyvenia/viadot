"""
'customer_gauge_to_adls.py'.

Prefect flow for the Customer Gauge Cloud API connector.

This module provides a prefect flow function to use the Customer Gauge connector:
- Call to the prefect task wrapper to get a final Data Frame from the connector.
- Upload that data to Azure Data Lake Storage.

Typical usage example:

    customer_gauge_to_adls(
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
        output_file_extension=output_file_extension,
        adls_path=adls_path,
        local_file_path=local_file_path,
        adls_azure_key_vault_secret=adls_azure_key_vault_secret,
        adls_sp_credentials_secret=adls_sp_credentials_secret,
        adls_config_key=adls_config_key,
        adls_path_overwrite=adls_path_overwrite,
        overwrite_adls=overwrite_adls,
        if_exists=if_exists,
        validate_df_dict=validate_df_dict,
        timeout=timeout,
    )

Functions:

    customer_gauge_to_adls(
        df=data_frame,
        path=adls_path,
        credentials=adls_sp_credentials_secret,
        credentials_secret=adls_azure_key_vault_secret,
        config_key=adls_config_key,
        overwrite=adls_path_overwrite,
        ): Flow to download data from Customer Gauge Cloud API and upload to ADLS.
"""

from datetime import datetime
from typing import Any, Dict, List, Literal, Optional

from prefect import flow

from viadot.orchestration.prefect.tasks import customer_gauge_to_df, df_to_adls


@flow
def customer_gauge_to_adls(
    *args: List[Any],
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
    output_file_extension: str = ".parquet",
    adls_path: str = None,
    local_file_path: str = None,
    adls_azure_key_vault_secret: str = None,
    adls_sp_credentials_secret: str = None,
    adls_config_key: Optional[str] = None,
    adls_path_overwrite: bool = False,
    overwrite_adls: bool = False,
    if_exists: str = "replace",
    validate_df_dict: dict = None,
    timeout: int = 3600,
    **kwargs: Dict[str, Any],
):
    """Flow for downloading data from the Customer Gauge's endpoints.

       (Responses and Non-Responses) via API to a CSV or Parquet file.
       The data anonymization is optional.Then upload it to Azure Data Lake.

    Args:
        name (str): The name of the flow.
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
        output_file_extension (str, optional): Output file extension - to allow
            selection of .csv for data which is not easy to handle with
            parquet. Defaults to ".parquet".
        adls_path (str, optional): Azure Data Lake destination
            folder/catalog path. Defaults to None.
        local_file_path (str, optional): Local destination path. Defaults to
            None.
        adls_azure_key_vault_secret (Optional[str], optional): The name of the
            Azure Key
        adls_sp_credentials_secret (str, optional): The name of the Azure Key
            Vault secret containing a dictionary with ACCOUNT_NAME and Service
            Principal credentials (TENANT_ID, CLIENT_ID, CLIENT_SECRET) for the
            Azure Data Lake. Defaults to None.
        adls_config_key (Optional[str], optional): The key in the viadot config holding
            relevant credentials. Defaults to None.
        adls_path_overwrite (bool, optional): Whether to overwrite the file in ADLS.
            Defaults to True.
        overwrite_adls (bool, optional): Whether to overwrite files in the
            lake. Defaults to False.
        if_exists (str, optional): What to do if the file exists. Defaults to
            "replace".
        validate_df_dict (Dict[str], optional): A dictionary with optional list of tests
            to verify the output dataframe. If defined, triggers the `validate_df`
            task from task_utils. Defaults to None.
        timeout (int, optional): The time (in seconds) to wait while running
            this task before a timeout occurs. Defaults to 3600.
    """
    data_frame = customer_gauge_to_df(
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
        customer_gauge_credentials_secret=customer_gauge_credentials_secret,
        anonymize=anonymize,
        columns_to_anonymize=columns_to_anonymize,
        anonymize_method=anonymize_method,
        anonymize_value=anonymize_value,
        date_column=date_column,
        days=days,
        output_file_extension=output_file_extension,
        adls_path=adls_path,
        local_file_path=local_file_path,
        adls_azure_key_vault_secret=adls_azure_key_vault_secret,
        adls_sp_credentials_secret=adls_sp_credentials_secret,
        adls_config_key=adls_config_key,
        adls_path_overwrite=adls_path_overwrite,
        overwrite_adls=overwrite_adls,
        if_exists=if_exists,
        validate_df_dict=validate_df_dict,
        timeout=timeout,
        **kwargs,
    )

    return df_to_adls(
        df=data_frame,
        path=adls_path,
        credentials=adls_sp_credentials_secret,
        credentials_secret=adls_azure_key_vault_secret,
        config_key=adls_config_key,
        overwrite=adls_path_overwrite,
    )
