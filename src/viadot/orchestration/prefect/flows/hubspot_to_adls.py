"""
'hubspot_to_adls.py'.

Prefect flow for the Hubspot API connector.

This module provides a prefect flow function to use the Hubspot connector:
- Call to the prefect task wrapper to get a final Data Frame from the connector.
- Upload that data to Azure Data Lake Storage.

Typical usage example:

    hubspot_to_adls(
        config_key=config_key,
        endpoint=endpoint,
        nrows=nrows,
        adls_credentials=adls_credentials,
        adls_path=adls_path,
        adls_path_overwrite=True,
    )


Functions:

    hubspot_to_adls(credentials, config_key, azure_key_vault_secret, endpoint,
        filters, properties, nrows, adls_credentials, adls_config_key,
        adls_azure_key_vault_secret, adls_path, adls_path_overwrite):
        Flow for downloading data from mindful to Azure Data Lake.
"""  # noqa: D412

from typing import Any, Dict, List, Optional

from prefect import flow
from prefect.task_runners import ConcurrentTaskRunner

from viadot.orchestration.prefect.tasks import df_to_adls, hubspot_to_df


@flow(
    name="Hubspot extraction to ADLS",
    description="Extract data from Hubspot API and load into Azure Data Lake Storage.",
    retries=1,
    retry_delay_seconds=60,
    task_runner=ConcurrentTaskRunner,
)
def hubspot_to_adls(
    credentials: Optional[Dict[str, Any]] = None,
    config_key: str = None,
    azure_key_vault_secret: Optional[str] = None,
    endpoint: Optional[str] = None,
    filters: Optional[List[Dict[str, Any]]] = None,
    properties: Optional[List[Any]] = None,
    nrows: int = 1000,
    adls_credentials: Optional[Dict[str, Any]] = None,
    adls_config_key: Optional[str] = None,
    adls_azure_key_vault_secret: Optional[str] = None,
    adls_path: Optional[str] = None,
    adls_path_overwrite: bool = False,
):
    """
    Flow for downloading data from mindful to Azure Data Lake.

    Args:
        credentials (Optional[Dict[str, Any]], optional): Genesys credentials as a
            dictionary. Defaults to None.
        config_key (str, optional): The key in the viadot config holding relevant
            credentials. Defaults to None.
        azure_key_vault_secret (Optional[str], optional): The name of the Azure Key
            Vault secret where credentials are stored. Defaults to None.
        endpoint (Optional[str], optional): API endpoint for an individual request.
            Defaults to None.
        filters (Optional[List[Dict[str, Any]]], optional): Filters defined for the API
            body in specific order. Defaults to None.
        properties (Optional[List[Any]], optional): List of user-defined columns to be
            pulled from the API. Defaults to None.
        nrows (int, optional): Max number of rows to pull during execution.
            Defaults to 1000.
        adls_credentials (Optional[Dict[str, Any]], optional): The credentials as a
            dictionary.Defaults to None.
        adls_config_key (Optional[str], optional): The key in the viadot config holding
            relevant credentials. Defaults to None.
        adls_azure_key_vault_secret (Optional[str], optional): The name of the Azure Key
            Vault secret containing a dictionary with ACCOUNT_NAME and Service Principal
            credentials (TENANT_ID, CLIENT_ID, CLIENT_SECRET) for the Azure Data Lake.
            Defaults to None.
        adls_path (Optional[str], optional): Azure Data Lake destination file path
            (with file name). Defaults to None.
        adls_path_overwrite (bool, optional): Whether to overwrite the file in ADLS.
            Defaults to True.
    """
    data_frame = hubspot_to_df(
        credentials=credentials,
        config_key=config_key,
        azure_key_vault_secret=azure_key_vault_secret,
        endpoint=endpoint,
        filters=filters,
        properties=properties,
        nrows=nrows,
    )

    df_to_adls(
        df=data_frame,
        path=adls_path,
        credentials=adls_credentials,
        credentials_secret=adls_azure_key_vault_secret,
        config_key=adls_config_key,
        overwrite=adls_path_overwrite,
    )
