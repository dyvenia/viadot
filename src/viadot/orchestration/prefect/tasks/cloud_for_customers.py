"""Tasks for interacting with Cloud for Customers."""

from typing import Any

import pandas as pd
from prefect import task

from viadot.orchestration.prefect.utils import get_credentials
from viadot.sources import CloudForCustomers


@task(retries=3, retry_delay_seconds=10, timeout_seconds=60 * 60)
def cloud_for_customers_to_df(
    url: str | None = None,
    endpoint: str | None = None,
    report_url: str | None = None,
    filter_params: dict[str, Any] | None = None,
    credentials_secret: str | None = None,
    config_key: str | None = None,
    **kwargs: dict[str, Any] | None,
) -> pd.DataFrame:
    """Extracts Cloud for Customers records as pd.DataFrame.

    Args:
        url (str, optional): The API url.
        endpoint (str, optional): The API endpoint.
        report_url (str, optional): The API url in case of prepared report.
        filter_params (dict[str, Any], optional): Query parameters.
        credentials_secret (str, optional): The name of the secret storing the
            credentials.
            More info on: https://docs.prefect.io/concepts/blocks/
        config_key (str, optional): The key in the viadot config holding relevant
            credentials.
        credentials (dict, optional): Cloud for Customers credentials.
        kwargs: The parameters to pass to DataFrame constructor.

    Returns:
        pd.Dataframe: The pandas `DataFrame` containing data from the file.
    """
    if not (credentials_secret or config_key):
        msg = "Either `credentials_secret` or `config_key` has to be specified and not empty."
        raise ValueError(msg)

    credentials = get_credentials(credentials_secret)
    c4c = CloudForCustomers(
        url=url,
        endpoint=endpoint,
        report_url=report_url,
        filter_params=filter_params,
        credentials=credentials,
        config_key=config_key,
    )
    return c4c.to_df(**kwargs)
