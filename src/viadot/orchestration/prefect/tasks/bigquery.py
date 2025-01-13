"""'bigquery.py'."""

import pandas as pd
from prefect import task

from viadot.orchestration.prefect.exceptions import MissingSourceCredentialsError
from viadot.orchestration.prefect.utils import get_credentials
from viadot.sources import BigQuery


@task(retries=3, log_prints=True, retry_delay_seconds=10, timeout_seconds=60 * 60)
def bigquery_to_df(
    config_key: str | None = None,
    azure_key_vault_secret: str | None = None,
    query: str | None = None,
    dataset_name: str | None = None,
    table_name: str | None = None,
    date_column_name: str | None = None,
    start_date: str | None = None,
    end_date: str | None = None,
    columns: list[str] | None = None,
) -> pd.DataFrame:
    """Task to download data from BigQuery API.

    Args:
        config_key (str, optional): The key in the viadot config holding relevant
            credentials. Defaults to None.
        azure_key_vault_secret (str, optional): The name of the Azure Key Vault secret
            where credentials are stored. Defaults to None.
        query (str): SQL query to querying data in BigQuery. Format of basic query:
            (SELECT * FROM `{project}.{dataset_name}.{table_name}`). Defaults to None.
        dataset_name (str, optional): Dataset name. Defaults to None.
        table_name (str, optional): Table name. Defaults to None.
        date_column_name (str, optional): The user can provide the name of the date. If
            the user-specified column does not exist, all data will be retrieved from
            the table. Defaults to None.
        start_date (str, optional): Parameter to pass start date e.g.
            "2022-01-01". Defaults to None.
        end_date (str, optional): Parameter to pass end date e.g.
            "2022-01-01". Defaults to None.
        columns (list[str], optional): List of columns from given table name.
                Defaults to None.

    Raises:
        MissingSourceCredentialsError: Credentials were not loaded.

    Returns:
        pd.DataFrame: The response data as a Pandas Data Frame.
    """
    if not (azure_key_vault_secret or config_key):
        raise MissingSourceCredentialsError

    if not config_key:
        credentials = get_credentials(azure_key_vault_secret)

    bigquery = BigQuery(credentials=credentials, config_key=config_key)

    return bigquery.to_df(
        query=query,
        dataset_name=dataset_name,
        table_name=table_name,
        date_column_name=date_column_name,
        start_date=start_date,
        end_date=end_date,
        columns=columns,
    )
