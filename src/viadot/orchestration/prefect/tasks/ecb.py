"""Task for downloading data from ECB API to a pandas DataFrame."""

from typing import Any, Literal

import pandas as pd
from prefect import task

from viadot.sources.ecb import ECB


@task(retries=3, log_prints=True, retry_delay_seconds=10, timeout_seconds=60 * 60)
def ecb_to_df(
    url: str,
    if_empty: Literal["warn", "skip", "fail"] = "warn",
    tests: dict[str, Any] | None = None,
) -> pd.DataFrame:
    """Task to download exchange rates data from ECB API to a pandas DataFrame.

    Args:
        url (str): The URL of the ECB API endpoint.
        if_empty (Literal["warn", "skip", "fail"], optional): What to do if the
            query returns no data. Defaults to "warn".
        tests (dict[str, Any], optional): A dictionary with optional list of tests
            to verify the output dataframe. If defined, triggers the `validate`
            function from viadot.utils. Defaults to None.

    Examples:
        data_frame = ecb_to_df(
            url="https://www.ecb.europa.eu/stats/eurofxref/eurofxref-daily.xml",
            if_empty="warn"
        )

    Returns:
        pd.DataFrame: The ECB exchange rates data as a pandas DataFrame with columns:
            - time: The date of the exchange rates
            - currency: The currency code (e.g., USD, GBP)
            - rate: The exchange rate against EUR
    """
    ecb = ECB()

    # Fetch and convert to DataFrame
    return ecb.to_df(
        url=url,
        if_empty=if_empty,
        tests=tests,
    )
