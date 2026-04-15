"""Task to download data from SAP BW into a Pandas DataFrame."""

import contextlib
from typing import Any

import pandas as pd
from prefect import task

from viadot.config import get_source_credentials
from viadot.orchestration.prefect.exceptions import MissingSourceCredentialsError
from viadot.orchestration.prefect.utils import get_credentials


with contextlib.suppress(ImportError):
    from viadot.sources import SAPBW


@task(retries=3, log_prints=True, retry_delay_seconds=10, timeout_seconds=60 * 60)
def sap_bw_to_df(
    mdx_query: str,
    config_key: str | None = None,
    azure_key_vault_secret: str | None = None,
    credentials: dict[str, Any] | None = None,
    mapping_dict: dict[str, Any] | None = None,
) -> pd.DataFrame:
    """Task to download data from SAP BW to DataFrame.

    Args:
        mdx_query (str, required): The MDX query to be passed to connection.
        config_key (Optional[str], optional): The key in the viadot config holding
            relevant credentials. Defaults to None.
        azure_key_vault_secret (Optional[str], optional): The name of the Azure Key
            Vault secret where credentials are stored. Defaults to None.
        credentials (dict[str, Any], optional): Credentials to SAP BW.
            If provided, this value has priority over `config_key`
            and `azure_key_vault_secret`. Defaults to None.
        mapping_dict (dict[str, Any], optional): Dictionary with original and new
            column names. Defaults to None.

    Raises:
        MissingSourceCredentialsError: If none credentials have been provided.


    Returns:
        pd.DataFrame: The response data as a Pandas Data Frame.
    """
    if not (credentials or config_key or azure_key_vault_secret):
        raise MissingSourceCredentialsError

    credentials = (
        credentials
        or get_source_credentials(config_key)
        or get_credentials(azure_key_vault_secret)
    )

    sap_bw = SAPBW(
        credentials=credentials,
        config_key=config_key,
    )
    sap_bw.api_connection(mdx_query=mdx_query)

    return sap_bw.to_df(mapping_dict=mapping_dict)
