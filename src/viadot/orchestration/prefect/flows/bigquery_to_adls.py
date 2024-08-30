"""
'bigquery_to_adls.py'.

Prefect flow for the BigQuery API connector.
This module provides a prefect flow function to use the BigQuery connector:
- Call to the prefect task wrapper to get a final Data Frame from the connector.
- Upload that data to Azure Data Lake Storage.

Typical usage example:
    flow = bigquery_to_adls(
        credentials=credentials,
        dataset_name=dataset_name,
        table_name=table_name,
        query=query,
        adls_path=adls_path,
        adls_azure_key_vault_secret=adls_azure_key_vault_secret,
        adls_path_overwrite=True,
    )

Functions:
    bigquery_to_adls(credentials, config_key, azure_key_vault_secret, query,
        dataset_name, table_name, date_column_name, start_date, end_date, columns,
        adls_credentials, adls_config_key, adls_azure_key_vault_secret, adls_path,
        adls_path_overwrite): Flow to download data from Outlook API to Azure Data Lake.
"""  # noqa: D412

from typing import Any, Dict, List, Optional

from prefect import flow
from prefect.task_runners import ConcurrentTaskRunner

from viadot.orchestration.prefect.tasks import bigquery_to_df, df_to_adls


@flow(
    name="BigQuery extraction to ADLS",
    description="Extract data from BigQuery and load it into Azure Data Lake Storage.",
    retries=1,
    retry_delay_seconds=60,
    task_runner=ConcurrentTaskRunner,
)
def bigquery_to_adls(
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
    adls_credentials: Optional[Dict[str, Any]] = None,
    adls_config_key: Optional[str] = None,
    adls_azure_key_vault_secret: Optional[str] = None,
    adls_path: Optional[str] = None,
    adls_path_overwrite: bool = False,
) -> None:
    """
    Download data from BigQuery to Azure Data Lake.

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
        adls_credentials (Optional[Dict[str, Any]], optional): The credentials as a
            dictionary. Defaults to None.
        adls_config_key (Optional[str], optional): The key in the viadot config holding
            relevant credentials. Defaults to None.
        adls_azure_key_vault_secret (Optional[str], optional): The name of the Azure Key
            Vault secret containing a dictionary with ACCOUNT_NAME and Service Principal
            credentials (TENANT_ID, CLIENT_ID, CLIENT_SECRET) for the Azure Data Lake.
            Defaults to None.
        adls_path (Optional[str], optional): Azure Data Lake destination file path.
            Defaults to None.
        adls_path_overwrite (bool, optional): Whether to overwrite the file in ADLS.
            Defaults to True.
    """
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

    return df_to_adls(
        df=data_frame,
        path=adls_path,
        credentials=adls_credentials,
        credentials_secret=adls_azure_key_vault_secret,
        config_key=adls_config_key,
        overwrite=adls_path_overwrite,
    )
