"""
'hubspot.py'.

Prefect task wrapper for the Hubspot API connector.

This module provides an intermediate wrapper between the prefect flow and the connector:
- Generate the Hubspot API connector.
- Create and return a pandas Data Frame with the response of the API.

Typical usage example:

    data_frame = hubspot_to_df(
        credentials=credentials,
        config_key=config_key,
        azure_key_vault_secret=azure_key_vault_secret,
        endpoint=endpoint,
        filters=filters,
        properties=properties,
        nrows=nrows,
    )

Functions:

    hubspot_to_df(credentials, config_key, azure_key_vault_secret, endpoint, filters,
        properties, nrows): Task to download data from Hubspot API.
"""  # noqa: D412

from typing import Any, Dict, List, Optional

import pandas as pd
from prefect import task

from viadot.exceptions import APIError
from viadot.orchestration.prefect.exceptions import MissingSourceCredentialsError
from viadot.orchestration.prefect.utils import get_credentials
from viadot.sources import Hubspot


@task(retries=3, log_prints=True, retry_delay_seconds=10, timeout_seconds=60 * 60)
def hubspot_to_df(
    credentials: Optional[Dict[str, Any]] = None,
    config_key: Optional[str] = None,
    azure_key_vault_secret: Optional[str] = None,
    endpoint: Optional[str] = None,
    filters: Optional[List[Dict[str, Any]]] = None,
    properties: Optional[List[Any]] = None,
    nrows: int = 1000,
) -> pd.DataFrame:
    """
    Task to download data from Hubspot API to Data Frame.

    Args:
        credentials (Optional[Dict[str, Any]], optional): Hubspot credentials as a
            dictionary. Defaults to None.
        config_key (Optional[str], optional): The key in the viadot config holding
            relevant credentials. Defaults to None.
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

    Raises:
        MissingSourceCredentialsError: If none credentials have been provided.
        APIError: The endpoint is a "must" requirement.

    Returns:
        pd.DataFrame: The response data as a Pandas Data Frame.
    """
    if not (azure_key_vault_secret or config_key or credentials):
        raise MissingSourceCredentialsError

    if not config_key:
        credentials = credentials or get_credentials(azure_key_vault_secret)

    if endpoint is None:
        raise APIError("Hubspot API endpoint is a mandatory requirement.")

    hubspot = Hubspot(
        credentials=credentials,
        config_key=config_key,
    )
    hubspot.api_connection(
        endpoint=endpoint,
        filters=filters,
        properties=properties,
        nrows=nrows,
    )
    data_frame = hubspot.to_df()

    return data_frame
