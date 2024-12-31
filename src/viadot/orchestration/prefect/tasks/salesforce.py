"""Task to download data from Salesforce API into a Pandas DataFrame."""

import pandas as pd
from prefect import task

from viadot.orchestration.prefect.exceptions import MissingSourceCredentialsError
from viadot.orchestration.prefect.utils import get_credentials
from viadot.sources import Salesforce


@task(retries=3, log_prints=True, retry_delay_seconds=10, timeout_seconds=60 * 60)
def salesforce_to_df(
    config_key: str | None = None,
    azure_key_vault_secret: str | None = None,
    env: str | None = None,
    domain: str | None = None,
    client_id: str | None = None,
    query: str | None = None,
    table: str | None = None,
    columns: list[str] | None = None,
) -> pd.DataFrame:
    """Querying Salesforce and saving data as the data frame.

    Args:
        config_key (str, optional): The key in the viadot config holding relevant
            credentials. Defaults to None.
        azure_key_vault_secret (str, optional): The name of the Azure Key Vault secret
            where credentials are stored. Defaults to None.
        env (str, optional): Environment information, provides information about
            credential and connection configuration. Defaults to 'DEV'.
        domain (str, optional): Domain of a connection. defaults to 'test' (sandbox).
            Can only be added if built-in username/password/security token is provided.
            Defaults to None.
        client_id (str, optional): Client id to keep the track of API calls.
            Defaults to None.
        query (str, optional): Query for download the data if specific download is
            needed. Defaults to None.
        table (str, optional): Table name. Can be used instead of query.
            Defaults to None.
        columns (list[str], optional): List of columns which are needed - table
            argument is needed. Defaults to None.

    Returns:
        pd.DataFrame: The response data as a pandas DataFrame.
    """
    if not (azure_key_vault_secret or config_key):
        raise MissingSourceCredentialsError

    if not config_key:
        credentials = get_credentials(azure_key_vault_secret)

    salesforce = Salesforce(
        credentials=credentials,
        config_key=config_key,
        env=env,
        domain=domain,
        client_id=client_id,
    )

    return salesforce.to_df(query=query, table=table, columns=columns)
