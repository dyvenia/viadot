"""Flow for transforming data inside the SQLServer."""

from prefect import flow

from viadot.orchestration.prefect.tasks import sql_server_query


@flow(
    name="transform--sql_server",
    description="Transform data inside the SQLServer.",
    retries=1,
    retry_delay_seconds=60,
)
def sql_server_transform(
    query: str,
    sql_server_credentials_secret: str | None = None,
    sql_server_config_key: str | None = None,
) -> None:
    """Run query inside the SQLServer.

    Args:
        query (str, required): The query to execute on the SQL Server database.
            If the qery doesn't start with "SELECT" returns an empty DataFrame.
        sql_server_credentials_secret (str, optional): The name of the secret storing
            the credentialsto the SQLServer. Defaults to None.
            More info on: https://docs.prefect.io/concepts/blocks/
        sql_server_config_key (str, optional): The key in the viadot config
            holding relevant credentials to the SQLServer. Defaults to None.
    """
    sql_server_query(
        query=query,
        config_key=sql_server_config_key,
        credentials_secret=sql_server_credentials_secret,
    )
