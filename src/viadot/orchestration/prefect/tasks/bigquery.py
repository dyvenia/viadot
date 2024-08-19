"""
'bigquery.py'.

Prefect task wrapper for the BigQuery API connector.
This module provides an intermediate wrapper between the prefect flow and the connector:
- Generate the BigQuery API connector.
- Create and return a pandas Data Frame with the response of the API.

Typical usage example:
    data_frame = bigquery_to_df(
        credentials=credentials,
        config_key=config_key,
        azure_key_vault_secret=azure_key_vault_secret,
        query=query,
        dataset_name=dataset_name,
        table_name=table_name,
        date_column_name=date_column_name,
        start_date=start_date,
        end_date=end_date,
        columns=columns,
    )

Functions:
    bigquery_to_df(credentials, config_key, azure_key_vault_secret, query, dataset_name,
    table_name, date_column_name, start_date, end_date, columns): Task to download data
    from BigQuery API.
"""  # noqa: D412

from typing import Any, Dict, List, Optional

import pandas as pd
from prefect import task

from viadot.orchestration.prefect.exceptions import MissingSourceCredentialsError
from viadot.orchestration.prefect.utils import get_credentials
from viadot.sources import BigQuery


@task(retries=3, log_prints=True, retry_delay_seconds=10, timeout_seconds=60 * 60)
def bigquery_to_df(
    credentials: Optional[Dict[str, Any]] = None,
    config_key: str = None,
    azure_key_vault_secret: Optional[str] = None,
    query: Optional[str] = None,
    dataset_name: Optional[str] = None,
    table_name: Optional[str] = None,
    date_column_name: Optional[str] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    columns: List[str] = [""],
) -> pd.DataFrame:
    """
    Task to download data from BigQuery API.

    Args:
        credentials (Optional[Dict[str, Any]], optional): Mediatool credentials as a
            dictionary. Defaults to None.
        config_key (str, optional): The key in the viadot config holding relevant
            credentials. Defaults to None.
        azure_key_vault_secret (Optional[str], optional): The name of the Azure Key
            Vault secret where credentials are stored. Defaults to None.
        query (Optional[str]): SQL query to querying data in BigQuery. Format of
                basic query: (SELECT * FROM `{project}.{dataset_name}.{table_name}`).
                Defaults to None.
        dataset_name (Optional[str], optional): Dataset name. Defaults to None.
        table_name (Optional[str], optional): Table name. Defaults to None.
        date_column_name (Optional[str], optional): The user can provide the name of
            the date. If the user-specified column does not exist, all data will be
            retrieved from the table. Defaults to None.
        start_date (Optional[str], optional): Parameter to pass start date e.g.
            "2022-01-01". Defaults to None.
        end_date (Optional[str], optional): Parameter to pass end date e.g.
            "2022-01-01". Defaults to None.
        columns (List[str], optional): List of columns from given table name.
                Defaults to [""].

    Raises:
        MissingSourceCredentialsError: Credentials were not loaded.

    Returns:
        pd.DataFrame: The response data as a Pandas Data Frame.
    """
    if not (azure_key_vault_secret or config_key or credentials):
        raise MissingSourceCredentialsError

    if not config_key:
        credentials = credentials or get_credentials(azure_key_vault_secret)

    bigquery = BigQuery(credentials=credentials, config_key=config_key)
    bigquery.api_connection(
        query=query,
        dataset_name=dataset_name,
        table_name=table_name,
        date_column_name=date_column_name,
        start_date=start_date,
        end_date=end_date,
        columns=columns,
    )
    data_frame = bigquery.to_df()

    return data_frame
