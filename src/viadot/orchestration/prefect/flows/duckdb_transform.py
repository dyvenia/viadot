"""Flow fortransforming data in the DuckDB."""
from typing import Any
from viadot.orchestration.prefect.tasks import duckdb_query

from prefect import flow


@flow(
    name="transform--duckdb",
    description="Transform data in the DuckDB.",
    retries=1,
    retry_delay_seconds=60,
)
def duckdb_transform(  # noqa: PLR0913, PLR0917
    query: str,
    duckdb_credentials_secret: str | None = None,
    # Specifing credentials in a dictionary is not recomended in the viadot flows, 
    # but in this case credantials can include only database name.
    duckdb_credentials: dict[str, Any] | None = None, 
    duckdb_config_key: str | None = None,
) -> None:
    """Transform data inside DuckDB database.

    Args:
        query (str, required): The query to execute on the DuckDB database.
        duckdb_credentials_secret (str, optional): The name of the secret storing
            the credentials to the DuckDB. Defaults to None.
            More info on: https://docs.prefect.io/concepts/blocks/
        duckdb_credentials (dict[str, Any], optional): Credentials to the DuckDB.
            Defaults to None.
        duckdb_config_key (str, optional): The key in the viadot config holding relevant
            credentials to the DuckDB. Defaults to None.

    """

    duckdb_query(
        query=query,
        credentials=duckdb_credentials,
        config_key=duckdb_config_key,
        credentials_secret=duckdb_credentials_secret,
    )
