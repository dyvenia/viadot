"""Task to download data from Mindful API into a Pandas DataFrame."""

from datetime import date
from typing import Literal

import pandas as pd
from prefect import get_run_logger, task

from viadot.orchestration.prefect.exceptions import MissingSourceCredentialsError
from viadot.orchestration.prefect.utils import get_credentials
from viadot.sources import Mindful


@task(retries=3, log_prints=True, retry_delay_seconds=10, timeout_seconds=60 * 60)
def mindful_to_df(
    config_key: str | None = None,
    azure_key_vault_secret: str | None = None,
    region: Literal["us1", "us2", "us3", "ca1", "eu1", "au1"] = "eu1",
    endpoint: str | None = None,
    date_interval: list[date] | None = None,
    limit: int = 1000,
) -> pd.DataFrame:
    """Task to download data from Mindful API.

    Args:
        config_key (str, optional): The key in the viadot config holding relevant
            credentials. Defaults to None.
        azure_key_vault_secret (Optional[str], optional): The name of the Azure Key
            Vault secret where credentials are stored. Defaults to None.
        region (Literal[us1, us2, us3, ca1, eu1, au1], optional): Survey Dynamix region
            from where to interact with the mindful API. Defaults to "eu1" English
            (United Kingdom).
        endpoint (Optional[str], optional): API endpoint for an individual request.
            Defaults to None.
        date_interval (Optional[List[date]], optional): Date time range detailing the
            starting date and the ending date. If no range is passed, one day of data
            since this moment will be retrieved. Defaults to None.
        limit (int, optional): The number of matching interactions to return.
            Defaults to 1000.

    Examples:
        data_frame = mindful_to_df(
            config_key=config_key,
            azure_key_vault_secret=azure_key_vault_secret,
            region=region,
            endpoint=end,
            date_interval=date_interval,
            limit=limit,
        )

    Returns:
        pd.DataFrame: The response data as a pandas DataFrame.
    """
    logger = get_run_logger()

    if not (azure_key_vault_secret or config_key):
        raise MissingSourceCredentialsError

    if not config_key:
        credentials = get_credentials(azure_key_vault_secret)

    if endpoint is None:
        logger.warning(
            "The API endpoint parameter was not defined. The default value is 'surveys'."
        )
        endpoint = "surveys"

    mindful = Mindful(
        credentials=credentials,
        config_key=config_key,
        region=region,
    )
    mindful.api_connection(
        endpoint=endpoint,
        date_interval=date_interval,
        limit=limit,
    )

    return mindful.to_df()
