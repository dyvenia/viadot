"""Download activity events for a day into a pandas DataFrame."""

from typing import Any

import pandas as pd
from prefect import task

from viadot.config import get_source_credentials
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
    config_key: str = "power_bi",
    credentials_secret: str | None = None,
) -> pd.DataFrame:
    """Download activity events for a day into a pandas DataFrame.

    Args:
        date (str, Optional): date string 'YYYY-MM-DD' (UTC day to extract).
        credentials (dict[str, Any], optional): Dict with 'client_id' and
            'client_secret' for OAuth 2.0 authentication. Defaults to None.
        config_key (str, optional): Key to look up credentials in viadot
            config. Defaults to "power_bi".
        credentials_secret (str, optional): Name of the AWS secret containing
            Power BI credentials. Defaults to None.

    Returns:
        pd.DataFrame: Flat DataFrame with Power BI activity events.
    """
    credentials = (
        credentials
        or get_source_credentials(config_key)
        or get_credentials(credentials_secret)
    )

    source = PowerBIActivityEvents(
        credentials=PowerBICredentials(**credentials),
        config_key=config_key,
    )
    return source.to_df(date=date)
