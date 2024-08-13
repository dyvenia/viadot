"""Task for downloading data from Hubspot API to a pandas DataFrame."""

from typing import Any

import pandas as pd
from prefect import task

from viadot.orchestration.prefect.exceptions import MissingSourceCredentialsError
from viadot.orchestration.prefect.utils import get_credentials
from viadot.sources import Hubspot


@task(retries=3, log_prints=True, retry_delay_seconds=10, timeout_seconds=60 * 60)
def hubspot_to_df(
    endpoint: str,
    config_key: str | None = None,
    azure_key_vault_secret: str | None = None,
    filters: list[dict[str, Any]] | None = None,
    properties: list[Any] | None = None,
    nrows: int = 1000,
) -> pd.DataFrame:
    """Task to download data from Hubspot API to a pandas DataFrame.

    Args:
        endpoint (str): API endpoint for an individual request.
        config_key (Optional[str], optional): The key in the viadot config holding
            relevant credentials. Defaults to None.
        azure_key_vault_secret (Optional[str], optional): The name of the Azure Key
            Vault secret where credentials are stored. Defaults to None.
        filters (Optional[List[Dict[str, Any]]], optional): Filters defined for the API
            body in specific order. Defaults to None.
        properties (Optional[List[Any]], optional): List of user-defined columns to be
            pulled from the API. Defaults to None.
        nrows (int, optional): Max number of rows to pull during execution.
            Defaults to 1000.

    Examples:
        data_frame = hubspot_to_df(
            config_key=config_key,
            azure_key_vault_secret=azure_key_vault_secret,
            endpoint=endpoint,
            filters=filters,
            properties=properties,
            nrows=nrows,
        )

    Raises:
        MissingSourceCredentialsError: If no credentials have been provided.

    Returns:
        pd.DataFrame: The response data as a pandas DataFrame.
    """
    if not (azure_key_vault_secret or config_key):
        raise MissingSourceCredentialsError

    if not config_key:
        credentials = get_credentials(azure_key_vault_secret)

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

    return hubspot.to_df()
