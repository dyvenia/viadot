"""Task for interacting with SAP."""

import contextlib
from typing import Any

import pandas as pd
from prefect import task
from prefect.logging import get_run_logger


with contextlib.suppress(ImportError):
    from viadot.sources import SAPRFC, SAPRFCV2
from viadot.orchestration.prefect.exceptions import MissingSourceCredentialsError
from viadot.orchestration.prefect.utils import get_credentials


@task(retries=3, retry_delay_seconds=10, timeout_seconds=60 * 60 * 3)
def sap_rfc_to_df(  # noqa: PLR0913
    query: str | None = None,
    sep: str | None = None,
    func: str | None = None,
    replacement: str = "-",
    rfc_total_col_width_character_limit: int = 400,
    rfc_unique_id: list[str] | None = None,
    tests: dict[str, Any] | None = None,
    credentials_secret: str | None = None,
    credentials: dict[str, Any] | None = None,
    config_key: str | None = None,
    alternative_version: bool = False,
) -> pd.DataFrame:
    """A task for querying SAP with SQL using the RFC protocol.

    Note that only a very limited subset of SQL is supported:
    - aliases
    - where clauses combined using the AND operator
    - limit & offset

    Unsupported:
    - aggregations
    - joins
    - subqueries
    - etc.

    Args:
        query (str): The query to be executed with pyRFC.
        sep (str, optional): The separator to use when reading query results. If not
            provided, multiple options are automatically tried. Defaults to None.
        func (str, optional): SAP RFC function to use. Defaults to None.
        replacement (str, optional): In case of sep is on a columns, set up a new
            character to replace inside the string to avoid flow breakdowns. Defaults to
            "-".
        rfc_total_col_width_character_limit (int, optional): Number of characters by
            which query will be split in chunks in case of too many columns for RFC
            function. According to SAP documentation, the limit is 512 characters.
            However, we observed SAP raising an exception even on a slightly lower
            number of characters, so we add a safety margin. Defaults to 400.
        rfc_unique_id (list[str], optional):
            Reference columns to merge chunks DataFrames. These columns must to be
            unique. If no columns are provided in this parameter, all data frame columns
            will by concatenated. Defaults to None.
        tests (dict[str], optional): A dictionary with optional list of tests
                to verify the output dataframe. If defined, triggers the `validate`
                function from viadot.utils. Defaults to None.
        credentials_secret (str, optional): The name of the secret that stores SAP
            credentials. Defaults to None.
            More info on: https://docs.prefect.io/concepts/blocks/
        credentials (dict[str, Any], optional): Credentials to SAP.
            Defaults to None.
        config_key (str, optional): The key in the viadot config holding relevant
            credentials. Defaults to None.
        alternative_version (bool, optional): Enable the use version 2 in source.
            Defaults to False.

    Examples:
        sap_rfc_to_df(
            ...
            rfc_unique_id=["VBELN", "LPRIO"],
            ...
        )
    """
    if not (credentials_secret or credentials or config_key):
        raise MissingSourceCredentialsError

    if query is None:
        msg = "Please provide the query."
        raise ValueError(msg)

    logger = get_run_logger()

    credentials = credentials or get_credentials(credentials_secret)

    if alternative_version is True:
        if rfc_unique_id:
            logger.warning(
                "If the column/set are not unique the table will be malformed."
            )
        sap = SAPRFCV2(
            sep=sep,
            replacement=replacement,
            credentials=credentials,
            func=func,
            rfc_total_col_width_character_limit=rfc_total_col_width_character_limit,
            rfc_unique_id=rfc_unique_id,
        )
    else:
        sap = SAPRFC(
            sep=sep,
            func=func,
            rfc_total_col_width_character_limit=rfc_total_col_width_character_limit,
            credentials=credentials,
            config_key=config_key,
        )
    sap.query(query)
    logger.info("Downloading data from SAP to a DataFrame...")
    logger.debug(f"Running query: \n{query}.")

    df = sap.to_df(tests=tests)

    if not df.empty:
        logger.info("Data has been downloaded successfully.")
    elif df.empty:
        logger.warn("Task finished but NO data was downloaded.")
    return df
