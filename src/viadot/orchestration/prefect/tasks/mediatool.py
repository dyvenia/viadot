"""'mediatool.py'."""

import pandas as pd
from prefect import task

from viadot.orchestration.prefect.exceptions import MissingSourceCredentialsError
from viadot.orchestration.prefect.utils import get_credentials
from viadot.sources import Mediatool


@task(retries=3, log_prints=True, retry_delay_seconds=10, timeout_seconds=60 * 60)
def mediatool_to_df(
    config_key: str | None = None,
    azure_key_vault_secret: str | None = None,
    organization_ids: list[str] | None = None,
    media_entries_columns: list[str] | None = None,
) -> pd.DataFrame:
    """Task to download data from Mediatool API.

    Args:
        config_key (str, optional): The key in the viadot config holding relevant
            credentials. Defaults to None.
        azure_key_vault_secret (str, optional): The name of the Azure Key Vault secret
            where credentials are stored. Defaults to None.
        organization_ids (list[str], optional): List of organization IDs.
            Defaults to None.
        media_entries_columns (list[str], optional): Columns to get from media entries.
            Defaults to None.

    Returns:
        pd.DataFrame: The response data as a Pandas Data Frame.
    """
    if not (azure_key_vault_secret or config_key):
        raise MissingSourceCredentialsError

    if not config_key:
        credentials = get_credentials(azure_key_vault_secret)

    mediatool = Mediatool(
        credentials=credentials,
        config_key=config_key,
    )

    return mediatool.to_df(organization_ids, media_entries_columns)
