"""Tasks for interacting with Microsoft Sharepoint."""

import pandas as pd
from prefect import get_run_logger, task

from viadot.orchestration.prefect.exceptions import MissingSourceCredentialsError
from viadot.orchestration.prefect.utils import get_credentials
from viadot.sources import EntraID


@task(retries=3, retry_delay_seconds=10, timeout_seconds=60 * 60)
def entraid_to_df(
    credentials_secret: str | None = None,
    config_key: str | None = None,
    max_concurrent: int = 50,
) -> pd.DataFrame:
    """Load data from EntraID into a pandas `DataFrame`.

    Modes:
    It downloads data from EntraID and creates a table from it.

    Args:
        credentials_secret (str, optional): The name of the secret storing
            the credentials for EntraID. Defaults to None.
            More info on: https://docs.prefect.io/concepts/blocks/
        config_key (str, optional): The key in the viadot config holding relevant
            credentials. Defaults to None.
        max_concurrent (int, optional): The maximum number of concurrent requests
            to EntraID. Defaults to 50.

    Returns:
        pd.Dataframe: The pandas `DataFrame` containing data from EntraID.

    """
    if not (credentials_secret or config_key):
        raise MissingSourceCredentialsError

    logger = get_run_logger()

    credentials = get_credentials(secret_name=credentials_secret)

    e = EntraID(
        credentials=credentials,
        config_key=config_key,
    )

    logger.info("Downloading data from EntraID...")
    df = e.to_df(
        if_empty="skip",
        max_concurrent=max_concurrent,
    )
    logger.info("Successfully downloaded data from EntraID.")

    return df

