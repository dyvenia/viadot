"""Flows for pulling data from Exchange rates API to Databricks."""

from datetime import datetime
from typing import Literal

from prefect import flow

from viadot.orchestration.prefect.tasks import df_to_databricks, exchange_rates_to_df


Currency = Literal[
    "USD", "EUR", "GBP", "CHF", "PLN", "DKK", "COP", "CZK", "SEK", "NOK", "ISK"
]


@flow(
    name="extract--exchange-rates-api--databricks",
    description="Extract data from Exchange Rates API and load it into Databricks.",
    retries=1,
    retry_delay_seconds=60,
)
def exchange_rates_to_databricks(  # noqa: PLR0913
    databricks_table: str,
    databricks_schema: str | None = None,
    if_exists: Literal["replace", "skip", "fail"] = "fail",
    currency: Currency = "USD",
    start_date: str = datetime.today().strftime("%Y-%m-%d"),
    end_date: str = datetime.today().strftime("%Y-%m-%d"),
    symbols: list[str] | None = None,
    exchange_rates_credentials_secret: str | None = None,
    exchange_rates_config_key: str | None = None,
    databricks_credentials_secret: str | None = None,
    databricks_config_key: str | None = None,
) -> None:
    """Download a DataFrame from ExchangeRates API and upload it to Databricks.

    Args:
        databricks_table (str): The name of the target table.
        databricks_schema (str, optional): The name of the target schema.
            Defaults to None.
        if_exists (Literal["replace", "skip", "fail"], optional):
            What to do if the table already exists.
            One of "replace", "skip", and "fail". Defaults to "fail".
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
            Only ISO codes.
        exchange_rates_credentials_secret (str, optional): The name of the
            Azure Key Vault secret storing the exchange rates credentials.
            Defaults to None.
        exchange_rates_config_key (str, optional): The key in the viadot config holding
            relevant credentials. Defaults to None.
        databricks_credentials_secret (str, optional): The name of the Azure Key Vault
            secret storing relevant credentials. Defaults to None.
        databricks_config_key (str, optional): The key in the viadot config holding
            relevant credentials. Defaults to None.
    """
    df = exchange_rates_to_df(
        currency=currency,
        credentials_secret=exchange_rates_credentials_secret,
        config_key=exchange_rates_config_key,
        start_date=start_date,
        end_date=end_date,
        symbols=symbols,
    )

    return df_to_databricks(
        df=df,
        schema=databricks_schema,
        table=databricks_table,
        if_exists=if_exists,
        credentials_secret=databricks_credentials_secret,
        config_key=databricks_config_key,
    )
