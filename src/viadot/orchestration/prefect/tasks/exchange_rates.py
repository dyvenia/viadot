"""Tasks for interacting with the Exchange Rates API."""

from datetime import datetime
from typing import Literal

import pandas as pd
from prefect import task

from viadot.orchestration.prefect.exceptions import MissingSourceCredentialsError
from viadot.orchestration.prefect.utils import get_credentials
from viadot.sources import ExchangeRates


Currency = Literal[
    "USD", "EUR", "GBP", "CHF", "PLN", "DKK", "COP", "CZK", "SEK", "NOK", "ISK"
]


@task(retries=3, retry_delay_seconds=10, timeout_seconds=60 * 60)
def exchange_rates_to_df(
    currency: Currency = "USD",
    credentials_secret: str | None = None,
    config_key: str | None = None,
    start_date: str = datetime.today().strftime("%Y-%m-%d"),
    end_date: str = datetime.today().strftime("%Y-%m-%d"),
    symbols: list[str] | None = None,
    tests: dict | None = None,
) -> pd.DataFrame:
    """Loads exchange rates from the Exchange Rates API into a pandas DataFrame.

    Args:
        currency (Currency, optional): Base currency to which prices of searched
            currencies are related. Defaults to "USD".
        credentials_secret (str, optional): The name of the secret storing
            the credentials. Defaults to None.
            More info on: https://docs.prefect.io/concepts/blocks/
        config_key (str, optional): The key in the viadot config holding relevant
            credentials.
            Defaults to None.
        start_date (str, optional): Initial date for data search.
            Data range is start_date -> end_date,
            supported format 'yyyy-mm-dd'.
            Defaults to datetime.today().strftime("%Y-%m-%d").
        end_date (str, optional): See above.
            Defaults to datetime.today().strftime("%Y-%m-%d").
        symbols (list[str], optional): List of ISO codes of currencies for which
            exchange rates from base currency will be fetched. Defaults to
            ["USD","EUR","GBP","CHF","PLN","DKK","COP","CZK","SEK","NOK","ISK"].
        tests (dict[str], optional): A dictionary with optional list of tests
            to verify the output dataframe. If defined, triggers the `validate` function
            from viadot.utils. Defaults to None.

    Returns:
        pd.DataFrame: The pandas `DataFrame` containing data from the file.
    """
    if not (credentials_secret or config_key):
        raise MissingSourceCredentialsError

    if not symbols:
        symbols = [
            "USD",
            "EUR",
            "GBP",
            "CHF",
            "PLN",
            "DKK",
            "COP",
            "CZK",
            "SEK",
            "NOK",
            "ISK",
        ]

    if not config_key:
        credentials = get_credentials(credentials_secret)

    e = ExchangeRates(
        currency=currency,
        start_date=start_date,
        end_date=end_date,
        symbols=symbols,
        credentials=credentials,
        config_key=config_key,
    )
    return e.to_df(tests=tests)
