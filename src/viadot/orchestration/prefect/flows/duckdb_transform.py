"""Flow for transforming data in DuckDB."""

from typing import Any

from prefect import flow

from viadot.orchestration.prefect.tasks import duckdb_query


@flow(
    name="transform--duckdb",
    description="Transform data in the DuckDB.",
    retries=1,
    retry_delay_seconds=60,
    timeout_seconds=2 * 60 * 60,
)
def duckdb_transform(
    query: str,
    duckdb_credentials_secret: str | None = None,
    # Specifying credentials in a dictionary is not recommended in the viadot flows,
    # but in this case credentials can include only database name.
    duckdb_credentials: dict[str, Any] | None = None,
    duckdb_config_key: str | None = None,
) -> None:
    """Transform data inside DuckDB database.

    Args:
        query (str, required): The query to execute on the DuckDB database.
        duckdb_credentials_secret (str, optional): The name of the secret storing
            the credentials. Defaults to None.
            More info on: https://docs.prefect.io/concepts/blocks/
        duckdb_credentials (dict[str, Any], optional): Credentials to the database.
            Defaults to None.
        duckdb_config_key (str, optional): The key in the viadot config holding relevant
            credentials. Defaults to None.
    """
    duckdb_query(
        query=query,
        credentials=duckdb_credentials,
        config_key=duckdb_config_key,
        credentials_secret=duckdb_credentials_secret,
    )
