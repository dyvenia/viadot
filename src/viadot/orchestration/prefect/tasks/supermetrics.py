"""Task for connecting to Supermetrics API."""

import pandas as pd
from prefect import task

from viadot.orchestration.prefect.exceptions import MissingSourceCredentialsError
from viadot.orchestration.prefect.utils import get_credentials
from viadot.sources import Supermetrics


@task(retries=3, log_prints=True, retry_delay_seconds=10, timeout_seconds=60 * 60)
def supermetrics_to_df(
    query_params: dict,
    config_key: str | None = None,
    credentials_secret: str | None = None,
) -> pd.DataFrame:
    """Task to retrive data from Supermetrics and returns it as a pandas DataFrame.

    This function queries the Supermetrics API using the provided query parameters and
    returns the data as a pandas DataFrame. The function supports both
    configuration-based and secret-based credentials.

    The function is decorated with a Prefect task, allowing it to handle retries,
    logging, and timeout behavior.

    Args:
    ----
        query_params (dict):
            A dictionary containing the parameters for querying the Supermetrics API.
            These parameters define what data to retrieve and how the query should
            be constructed.
        config_key (str, optional): The key in the viadot config holding relevant
            credentials. Defaults to None.
        credentials_secret (str, optional):
            The name of the secret in your secret management system that contains
            the Supermetrics API credentials. If `config_key` is not provided,
            this secret is used to authenticate with the Supermetrics API.

    Returns:
    -------
        pd.DataFrame:
            A pandas DataFrame containing the data retrieved from Supermetrics based
            on the provided query parameters.

    Raises:
    ------
        MissingSourceCredentialsError:
            Raised if neither `credentials_secret` nor `config_key` is provided,
            indicating that no valid credentials were supplied to access
            the Supermetrics API.

    """
    if not (credentials_secret or config_key):
        raise MissingSourceCredentialsError

    credentials = get_credentials(credentials_secret) if not config_key else None

    supermetrics = Supermetrics(
        credentials=credentials,
        config_key=config_key,
    )
    return supermetrics.to_df(query_params=query_params)
