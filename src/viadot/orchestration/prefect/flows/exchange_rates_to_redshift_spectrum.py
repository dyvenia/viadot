"""Download data from Exchange Rates API and upload it to AWS Redshift Spectrum."""

from datetime import datetime
from typing import Literal

from prefect import flow

from viadot.orchestration.prefect.tasks import (
    df_to_redshift_spectrum,
    exchange_rates_to_df,
)
from viadot.orchestration.prefect.tasks.exchange_rates import Currency


@flow(
    name="extract--exchange-rates-api--redshift_spectrum",
    description="Extract data from Exchange Rates API and load it into AWS Redshift Spectrum.",
    retries=1,
    retry_delay_seconds=60,
)
def exchange_rates_api_to_redshift_spectrum(  # noqa: PLR0913
    to_path: str,
    schema_name: str,
    table: str,
    currency: Currency = "USD",
    start_date: str = datetime.today().strftime("%Y-%m-%d"),
    end_date: str = datetime.today().strftime("%Y-%m-%d"),
    symbols: list[str] | None = None,
    if_exists: Literal["overwrite", "append"] = "overwrite",
    partition_cols: list[str] | None = None,
    compression: str | None = None,
    aws_config_key: str | None = None,
    aws_credentials_secret: str | None = None,
    exchange_rates_api_credentials_secret: str | None = None,
    exchange_rates_api_config_key: str | None = None,
) -> None:
    """Extract data from Exchange Rates API and load it into AWS Redshift Spectrum.

    Args:
        currency (Currency, optional): Base currency to which prices of searched
            currencies are related. Defaults to "USD".
        start_date (str, optional): Initial date for data search.
            Data range is start_date -> end_date,
            supported format 'yyyy-mm-dd'.
            Defaults to datetime.today().strftime("%Y-%m-%d").
        end_date (str, optional): See above.
            Defaults to datetime.today().strftime("%Y-%m-%d").
        symbols (list[str], optional): List of ISO codes of currencies for which
            exchange rates from base currency will be fetched. Defaults to
            ["USD","EUR","GBP","CHF","PLN","DKK","COP","CZK","SEK","NOK","ISK"].
        to_path (str): Path to a S3 folder where the table will be located. Defaults to
            None.
        schema_name (str): AWS Glue catalog database name.
        table (str): AWS Glue catalog table name.
        if_exists (str, optional): 'overwrite' to recreate any possible existing table
            or 'append' to keep any possible existing table. Defaults to overwrite.
        partition_cols (list[str], optional): List of column names that will be used to
            create partitions. Only takes effect if dataset=True. Defaults to None.
        compression (str, optional): Compression style (None, snappy, gzip, zstd).
        sep (str, optional): Field delimiter for the output file. Defaults to ','.
        aws_config_key (str, optional): The key in the viadot config holding relevant
            credentials. Defaults to None.
        aws_credentials_secret (str, optional): The name of a secret block in Prefect
            that stores AWS credentials. Defaults to None.
        exchange_rates_api_credentials_secret (str, optional): The name of the secret
            storing Exchange Rates API API key. Defaults to None.
            More info on: https://docs.prefect.io/concepts/blocks/
        exchange_rates_api_config_key (str, optional): The key in the viadot config
            holding relevant credentials. Defaults to None.
    """
    df = exchange_rates_to_df(
        currency=currency,
        start_date=start_date,
        end_date=end_date,
        symbols=symbols,
        credentials_secret=exchange_rates_api_credentials_secret,
        config_key=exchange_rates_api_config_key,
    )
    df_to_redshift_spectrum(
        df=df,
        to_path=to_path,
        schema_name=schema_name,
        table=table,
        if_exists=if_exists,
        partition_cols=partition_cols,
        compression=compression,
        config_key=aws_config_key,
        credentials_secret=aws_credentials_secret,
    )
