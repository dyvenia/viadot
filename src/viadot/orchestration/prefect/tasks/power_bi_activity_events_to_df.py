"""Download activity events for a day into a pandas DataFrame."""

from typing import Any

import pandas as pd
from prefect import task

from viadot.orchestration.prefect.utils import get_credentials
from viadot.sources.power_bi import PowerBIActivityEvents, PowerBICredentials


@task(
    name="power_bi_activity_events_to_df",
    description="Download activity events for a day into a pandas DataFrame.",
    retries=3,
    retry_delay_seconds=10,
    timeout_seconds=60 * 60 * 3,
)
def power_bi_activity_events_to_df(
    date: str | None = None,
    credentials: dict[str, Any] | None = None,
    credentials_secret: str | None = None,
    columns_to_extract: list[str] | None = None,
) -> pd.DataFrame:
    """Download activity events for a day into a pandas DataFrame.

    Args:
        date (str, Optional): date string 'YYYY-MM-DD' (UTC day to extract).
        credentials (dict[str, Any], optional): Dict with 'client_id' and
            'client_secret' for OAuth 2.0 authentication. Defaults to None.
        credentials_secret (str, optional): Name of the AWS secret containing
            Power BI credentials. Defaults to None.
        columns_to_extract (list(str)): List of columns to extract. Defaults to None.

    Returns:
        pd.DataFrame: Flat DataFrame with Power BI activity events.
    """
    credentials = credentials or get_credentials(credentials_secret)

    source = PowerBIActivityEvents(
        credentials=PowerBICredentials(**credentials),
        columns_to_extract=columns_to_extract,
    )
    return source.to_df(date=date)
