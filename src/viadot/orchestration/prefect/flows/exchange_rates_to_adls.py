"""Flows for pulling data from Exchange rates API to Azure Data Lake."""

from datetime import datetime
from typing import Literal

from prefect import flow

from viadot.orchestration.prefect.tasks import df_to_adls, exchange_rates_to_df


Currency = Literal[
    "USD", "EUR", "GBP", "CHF", "PLN", "DKK", "COP", "CZK", "SEK", "NOK", "ISK"
]


@flow(
    name="extract--exchange-rates-api--adls",
    description="Extract data from Exchange Rates API and load it into Azure Data Lake.",
    retries=1,
    retry_delay_seconds=60,
)
def exchange_rates_to_adls(
    adls_path: str,
    overwrite: bool = False,
    currency: Currency = "USD",
    start_date: str = datetime.today().strftime("%Y-%m-%d"),
    end_date: str = datetime.today().strftime("%Y-%m-%d"),
    symbols: list[str] | None = None,
    exchange_rates_credentials_secret: str | None = None,
    exchange_rates_config_key: str | None = None,
    adls_credentials_secret: str | None = None,
    adls_config_key: str | None = None,
) -> None:
    """Download a DataFrame from ExchangeRates API and upload it to Azure Data Lake.

    Args:
        adls_path (str): The destination path.
        overwrite (bool, optional): Whether to overwrite files in the lake.
            Defaults to False.
        currency (Currency, optional): Base currency to which prices of searched
            currencies are related. Defaults to "USD".
        start_date (str, optional): Initial date for data search.
            Data range is start_date -> end_date,
            supported format 'yyyy-mm-dd'.
            Defaults to datetime.today().strftime("%Y-%m-%d").
        end_date (str, optional): See above.
            Defaults to datetime.today().strftime("%Y-%m-%d").
        symbols (List[str], optional): List of currencies for which
            exchange rates from base currency will be fetched.
            Defaults to
            ["USD","EUR","GBP","CHF","PLN","DKK","COP","CZK","SEK","NOK","ISK"].
        exchange_rates_credentials_secret (str, optional): The name of the
            Azure Key Vault secret storing the exchange rates credentials.
            Defaults to None.
        exchange_rates_config_key (str, optional): The key in the viadot config holding
            relevant credentials. Defaults to None.
        adls_credentials_secret (str, optional): The name of the Azure Key Vault secret
            storing the ADLS credentials. Defaults to None.
        adls_config_key (str, optional): The key in the viadot config holding relevant
            credentials. Defaults to None.
    """
    df = exchange_rates_to_df(
        currency=currency,
        credentials_secret=exchange_rates_credentials_secret,
        config_key=exchange_rates_config_key,
        start_date=start_date,
        end_date=end_date,
        symbols=symbols,
    )

    return df_to_adls(
        df=df,
        path=adls_path,
        credentials_secret=adls_credentials_secret,
        config_key=adls_config_key,
        overwrite=overwrite,
    )
