"""Task for downloading data from Hubspot API to a pandas DataFrame."""

from typing import Any

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
    """
    Retrieves data from Supermetrics and returns it as a pandas DataFrame.

    This function queries the Supermetrics API using the provided query parameters and returns the data
    as a pandas DataFrame. The function supports both configuration-based and secret-based credentials.

    The function is decorated with a Prefect task, allowing it to handle retries, logging, and timeout behavior.

    Parameters
    ----------
    query_params : dict
        A dictionary containing the parameters for querying the Supermetrics API. These parameters
        define what data to retrieve and how the query should be constructed.
        
    config_key : str, optional
        The configuration key used to retrieve stored connection parameters and credentials for Supermetrics.
        If provided, this key is used to fetch configuration settings from a configuration file or environment.
        If not provided, the `credentials_secret` parameter must be used to supply credentials.

    credentials_secret : str, optional
        The name of the secret in your secret management system that contains the Supermetrics API credentials.
        If `config_key` is not provided, this secret is used to authenticate with the Supermetrics API.
        
    Returns
    -------
    pd.DataFrame
        A pandas DataFrame containing the data retrieved from Supermetrics based on the provided query parameters.

    Raises
    ------
    MissingSourceCredentialsError
        If neither `credentials_secret` nor `config_key` is provided, the function raises this error,
        indicating that no valid credentials were supplied to access the Supermetrics API.

    Notes
    -----
    - The function is decorated with Prefect's `@task` decorator, which allows it to be used within a Prefect flow.
    - The task is configured to retry up to three times with a delay of 10 seconds between retries if it encounters
      an error. The task will timeout if it runs longer than 60 minutes.
    - This function assumes that the `Supermetrics` class is initialized with either credentials or a configuration key.
    """
    
    
    if not (credentials_secret or config_key):
        raise MissingSourceCredentialsError

    if not config_key:
        credentials = get_credentials(credentials_secret)

    supermetrics = Supermetrics(
        credentials=credentials,
        config_key=config_key,
    )
    return supermetrics.to_df(query_params=query_params)

