"""Tasks for interacting with MySQL."""

from typing import Any

import pandas as pd
from prefect import task
from prefect.logging import get_run_logger

from viadot.config import get_source_credentials
from viadot.orchestration.prefect.exceptions import MissingSourceCredentialsError
from viadot.orchestration.prefect.utils import get_credentials
from viadot.sources.mysql import MySQL


def _to_mysql_credentials(raw: dict[str, Any]) -> dict[str, Any]:
    """Map Prefect secret keys onto MySQL connector fields.

    Args:
        raw (dict[str, Any]): Credentials from a Prefect secret (``uid``, ``pwd``,
            ``server``, ``port``, ``db``, ``sslmode``) or already in connector form.

    Returns:
        dict[str, Any]: A new dict with ``user`` taken from ``uid`` when ``uid`` is
        present. Otherwise ``raw`` unchanged, so an existing ``user`` key is kept.
    """
    if "uid" not in raw:
        return raw
    return {
        "user": raw["uid"],
        "password": raw["pwd"],
        "server": raw["server"],
        "port": int(raw.get("port") or 3306),
        "db_name": raw["db"],
        "sslmode": raw.get("sslmode") or None,
    }


@task(retries=3, retry_delay_seconds=10, timeout_seconds=60 * 60 * 3)
def mysql_to_df(
    query: str,
    credentials_secret: str | None = None,
    credentials: dict[str, Any] | None = None,
    config_key: str | None = None,
    tests: dict[str, Any] | None = None,
) -> pd.DataFrame:
    """Load the result of a MySQL query into a pandas DataFrame.

    Args:
        query (str): The query to execute on the MySQL database.
        credentials_secret (str, optional): The name of the Prefect secret storing
            the credentials (server, port, uid, pwd, db, sslmode). Defaults to None.
            More info on: https://docs.prefect.io/concepts/blocks/
        credentials (dict[str, Any], optional): Credentials to MySQL. If provided,
            this value has priority over `config_key` and `credentials_secret`.
            Defaults to None.
        config_key (str, optional): The key in the viadot config holding relevant
            credentials. Defaults to None.
        tests (dict[str, Any], optional): A dictionary with optional list of tests
            to verify the output dataframe. Defaults to None.

    Returns:
        pd.DataFrame: The resulting data as a pandas DataFrame.
    """
    if not (credentials or config_key or credentials_secret):
        raise MissingSourceCredentialsError

    logger = get_run_logger()

    raw = (
        credentials
        or get_source_credentials(config_key)
        or get_credentials(credentials_secret)
    )
    mysql = MySQL(credentials=_to_mysql_credentials(raw))
    df = mysql.to_df(query=query, tests=tests)

    nrows = df.shape[0]
    ncols = df.shape[1]
    logger.info(
        f"Successfully downloaded {nrows} rows and {ncols} columns of data to a DataFrame."
    )
    return df
