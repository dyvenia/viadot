"""Task for downloading data from Aselite."""

from typing import Any, Dict, List, Literal
import pandas as pd

from prefect import task
from viadot.orchestration.prefect.utils import get_credentials
from viadot.orchestration.prefect.tasks.task_utils import (
    df_clean_column,
    df_to_csv,
    df_converts_bytes_to_int
)
from viadot.utils import validate
from viadot.sources import AzureSQL


@task(retries=3, retry_delay_seconds=10, timeout_seconds=60 * 60)
def aselite_to_df(
    query: str = None,
    credentials_secret: str = None,
    sep: str = "\t",
    file_path: str = None,
    if_exists: Literal["replace", "append", "delete"] = "replace",
    validate_df_dict: Dict[str, Any] = None,
    convert_bytes: bool = False,
    remove_special_characters: bool = None,
    columns_to_clean: List[str] = None,

) -> pd.DataFrame:
    r"""Task to download data from Aselite.

    Args:
        query (str): Query to perform on a database. Defaults to None.
        credentials_secret (str, optional): The name of the Azure Key Vault
            secret containing a dictionary with ASElite SQL Database credentials.
            Defaults to None.
        sep (str, optional): The delimiter for the output CSV file. Defaults to "\t".
        file_path (str, optional): Local destination path. Defaults to None.
        if_exists (Literal, optional): What to do if the table exists.
            Defaults to "replace".
        validate_df_dict (Dict[str], optional): A dictionary with optional list of
            tests to verify the output dataframe. If defined, triggers the `validate_df`
            task from task_utils. Defaults to None.
        convert_bytes (bool). A boolean value to trigger method df_converts_bytes_to_int
            Defaults to False.
        remove_special_characters (str, optional): Call a function that remove
            special characters like escape symbols. Defaults to None.
        columns_to_clean (List(str), optional): Select columns to clean, used with
            remove_special_characters. If None whole data frame will be processed.
            Defaults to None.

    Raises:
        ValueError: Raising ValueError if credentials_secret is not provided

    Returns:
        pd.DataFrame: The response data as a pandas DataFrame.
    """
    if not credentials_secret:
        msg = "`credentials_secret` has to be specified and not empty."
        raise ValueError(msg)

    credentials = get_credentials(credentials_secret)

    aselite = AzureSQL(credentials=credentials)

    df = aselite.to_df(query=query)

    if convert_bytes:
        df = df_converts_bytes_to_int(df=df)

    if remove_special_characters:
        df = df_clean_column(df=df, columns_to_clean=columns_to_clean)

    if validate_df_dict is not None:
        validate(df=df, tests=validate_df_dict)

    csv_output = df_to_csv(df=df, path=file_path, sep=sep, if_exists=if_exists)

    return csv_output
