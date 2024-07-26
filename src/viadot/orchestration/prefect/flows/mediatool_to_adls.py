"""
'mediatool_to_adls.py'.

Prefect flow for the Mediatool API connector.

This module provides a prefect flow function to use the Mediatool connector:
- Call to the prefect task wrapper to get a final Data Frame from the connector.
- Upload that data to Azure Data Lake Storage.

Typical usage example:
    mediatool_to_adls(
        azure_key_vault_secret=azure_key_vault_secret,
        organization_ids=organization_ids,
        media_entries_columns=media_entries_columns,
        adls_path=adls_path,
        adls_azure_key_vault_secret=adls_azure_key_vault_secret,
        adls_path_overwrite=True,
    )

Functions:
    mediatool_to_adls(credentials, config_key, azure_key_vault_secret, organization_ids,
        media_entries_columns, adls_credentials, adls_config_key,
        adls_azure_key_vault_secret, adls_path, adls_path_overwrite):
        Flow for downloading data from Mediatool to Azure Data Lake.
"""  # noqa: D412

from typing import Any, Dict, List, Optional

from prefect import flow
from prefect.task_runners import ConcurrentTaskRunner

from viadot.orchestration.prefect.tasks import df_to_adls, mediatool_to_df


@flow(
    name="Mediatool extraction to ADLS",
    description="Extract data from Mediatool and load it into Azure Data Lake Storage.",
    retries=1,
    retry_delay_seconds=60,
    task_runner=ConcurrentTaskRunner,
)
def mediatool_to_adls(
    credentials: Optional[Dict[str, Any]] = None,
    config_key: str = None,
    azure_key_vault_secret: Optional[str] = None,
    organization_ids: List[str] = None,
    media_entries_columns: Optional[List[str]] = None,
    adls_credentials: Optional[Dict[str, Any]] = None,
    adls_config_key: Optional[str] = None,
    adls_azure_key_vault_secret: Optional[str] = None,
    adls_path: Optional[str] = None,
    adls_path_overwrite: bool = False,
) -> None:
    """
    Download data from Mediatool to Azure Data Lake.

    Args:
        credentials (Optional[Dict[str, Any]], optional): Mediatool credentials as a
            dictionary. Defaults to None.
        config_key (str, optional): The key in the viadot config holding relevant
            credentials. Defaults to None.
        azure_key_vault_secret (Optional[str], optional): The name of the Azure Key
            Vault secret where credentials are stored. Defaults to None.
        organization_ids (List[str], optional): List of organization IDs.
            Defaults to None.
        media_entries_columns (Optional[List[str]], optional): Columns to get from
                media entries. Defaults to None.
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
    data_frame = mediatool_to_df(
        credentials=credentials,
        config_key=config_key,
        azure_key_vault_secret=azure_key_vault_secret,
        organization_ids=organization_ids,
        media_entries_columns=media_entries_columns,
    )

    return df_to_adls(
        df=data_frame,
        path=adls_path,
        credentials=adls_credentials,
        credentials_secret=adls_azure_key_vault_secret,
        config_key=adls_config_key,
        overwrite=adls_path_overwrite,
    )
