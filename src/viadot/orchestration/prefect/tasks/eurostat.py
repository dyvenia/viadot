"""Tasks for interacting with Eurostat."""

import pandas as pd
from prefect import task

from viadot.sources import Eurostat


@task(retries=3, retry_delay_seconds=10, timeout_seconds=60 * 60)
def eurostat_to_df(
    dataset_code: str,
    params: dict[str, str] | None = None,
    columns: list[str] | None = None,
    tests: dict | None = None,
) -> pd.DataFrame:
    """Task for creating a pandas DataFrame from Eurostat HTTPS REST API.

    This function serves as an intermediate wrapper between the prefect flow
    and the Eurostat connector:
    - Instantiates an Eurostat Cloud API connector.
    - Creates and returns a pandas DataFrame with the response from the API.

    Args:
        dataset_code (str): The code of the Eurostat dataset that you would like
            to download.
        params (dict[str, str] | None, optional):
            A dictionary with optional URL parameters. Each key is a parameter ID,
            and the value is a specific parameter code, e.g.,
            `params = {'unit': 'EUR'}` where "unit" is the parameter, and "EUR" is
            the code. Only one code per parameter is allowed. Defaults to None.
        columns (list[str] | None, optional):
            A list of column names (as strings) that are required from the dataset.
            Filters the DataFrame to only include the specified columns.
            Defaults to None.
        tests (dict | None, optional):
            A dictionary containing test cases for the data, including:
            - `column_size`: dict{column: size}
            - `column_unique_values`: list[columns]
            - `column_list_to_match`: list[columns]
            - `dataset_row_count`: dict{'min': number, 'max': number}
            - `column_match_regex`: dict{column: 'regex'}
            - `column_sum`: dict{column: {'min': number, 'max': number}}.
            Defaults to None.

    Returns:
        pd.DataFrame:
            A pandas DataFrame containing the data retrieved from the Eurostat API.
    """
    return Eurostat(
        dataset_code=dataset_code, params=params, columns=columns, tests=tests
    ).to_df()
