"""
'genesys_to_adls.py'.

Prefect flow for the Genesys Cloud API connector.

This module provides a prefect flow function to use the Genesys connector:
- Call to the prefect task wrapper to get a final Data Frame from the connector.
- Upload that data to Azure Data Lake Storage.

Typical usage example:

    genesys_to_adls(
        credentials=credentials,
        verbose=False,
        endpoint=endpoint,
        post_data_list=data_to_post,
        adls_credentials=adls_credentials,
        adls_path=adls_path,
        adls_path_overwrite=True,
    )

Functions:

    genesys_to_adls(credentials, config_key, azure_key_vault_secret, verbose,
        endpoint, environment, queues_ids, view_type, view_type_time_sleep,
        post_data_list, normalization_sep, drop_duplicates, validate_df_dict,
        adls_credentials, adls_config_key, adls_azure_key_vault_secret,
        adls_path, adls_path_overwrite): Flow to download data from Genesys Cloud API
        and upload to ADLS.
"""  # noqa: D412

import time
from typing import Any, Dict, List, Optional

from prefect import flow
from prefect.task_runners import ConcurrentTaskRunner

from viadot.orchestration.prefect.tasks import df_to_adls, genesys_to_df


@flow(
    name="Genesys extraction to ADLS",
    description="Extract data from Genesys Cloud"
    + " and load it into Azure Data Lake Storage.",
    retries=1,
    retry_delay_seconds=60,
    task_runner=ConcurrentTaskRunner,
    log_prints=True,
)
def genesys_to_adls(
    credentials: Optional[Dict[str, Any]] = None,
    config_key: str = None,
    azure_key_vault_secret: Optional[str] = None,
    verbose: Optional[bool] = None,
    endpoint: Optional[str] = None,
    environment: str = "mypurecloud.de",
    queues_ids: Optional[List[str]] = None,
    view_type: Optional[str] = None,
    view_type_time_sleep: Optional[int] = None,
    post_data_list: Optional[List[Dict[str, Any]]] = None,
    normalization_sep: str = ".",
    drop_duplicates: bool = False,
    validate_df_dict: Optional[Dict[str, Any]] = None,
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
        verbose (bool, optional): Increase the details of the logs printed on the
                screen. Defaults to False.
        endpoint (Optional[str], optional): Final end point to the API.
            Defaults to None.
        environment (str, optional): the domain that appears for Genesys Cloud
            Environment based on the location of your Genesys Cloud organization.
            Defaults to "mypurecloud.de".
        queues_ids (Optional[List[str]], optional): List of queues ids to consult the
                members. Defaults to None.
        view_type (Optional[str], optional): The type of view export job to be created.
            Defaults to None.
        view_type_time_sleep (Optional[int], optional): Waiting time to retrieve data
            from Genesys Cloud API. Defaults to None.
        post_data_list (Optional[List[Dict[str, Any]]], optional): List of string
            templates to generate json body in POST calls to the API. Defaults to None.
        normalization_sep (str, optional): Nested records will generate names separated
            by sep. Defaults to ".".
        drop_duplicates (bool, optional): Remove duplicates from the Data Frame.
            Defaults to False.
        validate_df_dict (Optional[Dict[str, Any]], optional): A dictionary with
            optional list of tests to verify the output dataframe. Defaults to None.
        adls_credentials (Optional[Dict[str, Any]], optional): The credentials as a
            dictionary. Defaults to None.
        adls_config_key (Optional[str], optional): The key in the viadot config holding
            relevant credentials. Defaults to None.
        adls_azure_key_vault_secret (Optional[str], optional): The name of the Azure Key
            Vault secret containing a dictionary with ACCOUNT_NAME and Service Principal
            credentials (TENANT_ID, CLIENT_ID, CLIENT_SECRET) for the Azure Data Lake.
            Defaults to None.
        adls_path (Optional[str], optional): Azure Data Lake destination file path (with
            file name). Defaults to None.
        adls_path_overwrite (bool, optional): Whether to overwrite the file in ADLS.
            Defaults to True.
    """
    data_frame = genesys_to_df(
        credentials=credentials,
        config_key=config_key,
        azure_key_vault_secret=azure_key_vault_secret,
        verbose=verbose,
        endpoint=endpoint,
        environment=environment,
        queues_ids=queues_ids,
        view_type=view_type,
        view_type_time_sleep=view_type_time_sleep,
        post_data_list=post_data_list,
        normalization_sep=normalization_sep,
        drop_duplicates=drop_duplicates,
        validate_df_dict=validate_df_dict,
    )
    time.sleep(0.5)

    df_to_adls(
        df=data_frame,
        path=adls_path,
        credentials=adls_credentials,
        credentials_secret=adls_azure_key_vault_secret,
        config_key=adls_config_key,
        overwrite=adls_path_overwrite,
    )
