"""Task for downloading data from Azure SQL."""

from typing import Any, Literal

import pandas as pd
from prefect import task

from viadot.orchestration.prefect.utils import get_credentials
from viadot.sources import AzureSQL
from viadot.utils import validate


@task(retries=3, retry_delay_seconds=10, timeout_seconds=60 * 60)
def azure_sql_to_df(
    query: str | None = None,
    credentials_secret: str | None = None,
    validate_df_dict: dict[str, Any] | None = None,
    convert_bytes: bool = False,
    remove_special_characters: bool | None = None,
    columns_to_clean: list[str] | None = None,
    if_empty: Literal["warn", "skip", "fail"] = "warn",
) -> pd.DataFrame:
    r"""Task to download data from Azure SQL.

    Args:
        query (str): Query to perform on a database. Defaults to None.
        credentials_secret (str, optional): The name of the Azure Key Vault
            secret containing a dictionary with database credentials.
            Defaults to None.
        validate_df_dict (Dict[str], optional): A dictionary with optional list of
            tests to verify the output dataframe. If defined, triggers the `validate_df`
            task from task_utils. Defaults to None.
        convert_bytes (bool). A boolean value to trigger method df_converts_bytes_to_int
            It is used to convert bytes data type into int, as pulling data with bytes
            can lead to malformed data in data frame.
            Defaults to False.
        remove_special_characters (str, optional): Call a function that remove
            special characters like escape symbols. Defaults to None.
        columns_to_clean (List(str), optional): Select columns to clean, used with
            remove_special_characters. If None whole data frame will be processed.
            Defaults to None.
        if_empty (Literal["warn", "skip", "fail"], optional): What to do if the
            query returns no data. Defaults to None.

    Raises:
        ValueError: Raising ValueError if credentials_secret is not provided

    Returns:
        pd.DataFrame: The response data as a pandas DataFrame.
    """
    if not credentials_secret:
        msg = "`credentials_secret` has to be specified and not empty."
        raise ValueError(msg)

    credentials = get_credentials(credentials_secret)

    azure_sql = AzureSQL(credentials=credentials)

    df = azure_sql.to_df(
        query=query,
        if_empty=if_empty,
        convert_bytes=convert_bytes,
        remove_special_characters=remove_special_characters,
        columns_to_clean=columns_to_clean,
    )

    if validate_df_dict is not None:
        validate(df=df, tests=validate_df_dict)

    return df
