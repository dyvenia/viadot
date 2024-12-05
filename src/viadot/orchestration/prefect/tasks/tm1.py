"""Task for downloading data from TM1 to a pandas DataFrame."""

from pandas import DataFrame
from prefect import task
from prefect.logging import get_run_logger

from viadot.config import get_source_credentials
from viadot.orchestration.prefect.exceptions import MissingSourceCredentialsError
from viadot.orchestration.prefect.utils import get_credentials
from viadot.sources.tm1 import TM1


@task(retries=3, retry_delay_seconds=10, timeout_seconds=60 * 60 * 3)
def tm1_to_df(
    mdx_query: str | None = None,
    cube: str | None = None,
    view: str | None = None,
    limit: int | None = None,
    private: bool = False,
    credentials_secret: str | None = None,
    config_key: str | None = None,
    verify: bool = False,
    if_empty: str = "skip",
) -> DataFrame:
    """Download data from TM1 to pandas DataFrame.

    Args:
        mdx_query (str, optional): MDX select query needed to download the data.
            Defaults to None.
        cube (str, optional): Cube name from which data will be downloaded.
            Defaults to None.
        view (str, optional): View name from which data will be downloaded.
            Defaults to None.
        limit (str, optional): How many rows should be extracted.
            If None all the available rows will be downloaded. Defaults to None.
        private (bool, optional): Whether or not data download should be private.
            Defaults to False.
        credentials_secret (dict[str, Any], optional): The name of the secret that
            stores TM1 credentials.
            More info on: https://docs.prefect.io/concepts/blocks/. Defaults to None.
        config_key (str, optional): The key in the viadot config holding relevant
            credentials. Defaults to "TM1".
        verify (bool, optional): Whether or not verify SSL certificate.
            Defaults to False.
        if_empty (str, optional): What to do if output DataFrame is empty.
            Defaults to "skip".

    """
    if not (credentials_secret or config_key):
        raise MissingSourceCredentialsError

    logger = get_run_logger()

    credentials = get_source_credentials(config_key) or get_credentials(
        credentials_secret
    )

    bc = TM1(
        credentials=credentials,
        config_key=config_key,
        limit=limit,
        private=private,
        verify=verify,
    )

    df = bc.to_df(
        if_empty=if_empty,
        mdx_query=mdx_query,
        cube=cube,
        view=view,
    )

    nrows = df.shape[0]
    ncols = df.shape[1]

    logger.info(
        f"Successfully downloaded {nrows} rows and {ncols} columns of data to a DataFrame."
    )

    return df
