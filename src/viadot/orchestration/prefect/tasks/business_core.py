"""Task for downloading data from Business Core API to a pandas DataFrame."""

from typing import Any

from pandas import DataFrame
from prefect import task
from prefect.logging import get_run_logger

from viadot.config import get_source_credentials
from viadot.orchestration.prefect.exceptions import MissingSourceCredentialsError
from viadot.orchestration.prefect.utils import get_credentials
from viadot.sources.business_core import BusinessCore


@task(retries=3, retry_delay_seconds=10, timeout_seconds=60 * 60 * 3)
def business_core_to_df(
    url: str | None = None,
    filters: dict[str, Any] | None = None,
    credentials_secret: str | None = None,
    config_key: str | None = None,
    if_empty: str = "skip",
    verify: bool = True,
) -> DataFrame:
    """Download data from Business Core API to a pandas DataFrame.

    Args:
        url (str, required): Base url to the view in Business Core API. Defaults to
            None.
        filters (dict[str, Any], optional): Filters in form of dictionary. Available
            filters: 'BucketCount','BucketNo', 'FromDate', 'ToDate'. Defaults to None.
        credentials_secret (str, optional): The name of the secret that stores Business
            Core credentials. More info on: https://docs.prefect.io/concepts/blocks/.
            Defaults to None.
        config_key (str, optional): The key in the viadot config holding relevant
            credentials. Defaults to None.
        if_empty (str, optional): What to do if output DataFrame is empty. Defaults to
            "skip".
        verify (bool, optional): Whether or not verify certificates while connecting
            to an API. Defaults to True.
    """
    if not (credentials_secret or config_key):
        raise MissingSourceCredentialsError

    logger = get_run_logger()

    credentials = get_source_credentials(config_key) or get_credentials(
        credentials_secret
    )

    bc = BusinessCore(
        url=url,
        credentials=credentials,
        config_key=config_key,
        filters=filters,
        verify=verify,
    )

    df = bc.to_df(if_empty=if_empty)

    nrows = df.shape[0]
    ncols = df.shape[1]

    logger.info(
        f"Successfully downloaded {nrows} rows and {ncols} columns of data to a DataFrame."
    )

    return df
