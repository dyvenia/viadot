"""Tasks for interacting with Cloud for Customers."""

from typing import Any

import pandas as pd
from viadot.orchestration.prefect.utils import get_credentials
from viadot.sources import CloudForCustomers

from prefect import task


@task(retries=3, retry_delay_seconds=10, timeout_seconds=60 * 60)
def cloud_for_customers_to_df(  # noqa: PLR0913, PLR0917
    url: str | None = None,
    fields: list[str] | None = None,
    dtype: dict[str, Any] | None = None,
    endpoint: str | None = None,
    report_url: str | None = None,
    filter_params: dict[str, Any] | None = None,
    tests: dict[str, Any] | None = None,
    credentials_secret: str | None = None,
    config_key: str | None = None,
    credentials: dict[str, Any] | None = None,
    **kwargs: dict[str, Any] | None,
) -> pd.DataFrame:
    """Extracts Cloud for Customers records as pd.DataFrame.

    Args:
        url (str, optional): The API url.
        fields (list[str], optional): List of fields to put in DataFrame.
        dtype (dict, optional): The dtypes to use in the DataFrame.
        endpoint (str, optional): The API endpoint.
        report_url (str, optional): The API url in case of prepared report.
        filter_params (dict[str, Any], optional): Query parameters.
        credentials_secret (str, optional): The name of the secret storing the
            credentials.
            More info on: https://docs.prefect.io/concepts/blocks/
        tests (dict[str], optional): A dictionary with optional list of tests
            to verify the output dataframe. If defined, triggers the `validate`
            function from viadot.utils. Defaults to None.
        config_key (str, optional): The key in the viadot config holding relevant
            credentials.
        credentials (dict, optional): Cloud for Customers credentials.
        kwargs: The parameters to pass to DataFrame constructor.

    Returns:
        pd.Dataframe: The pandas `DataFrame` containing data from the file.
    """
    if not (credentials_secret or config_key or credentials):
        msg = "Either `credentials_secret`, `config_key`, or `credentials` has to be specified and not empty."
        raise ValueError(msg)

    credentials = credentials or get_credentials(credentials_secret)
    c4c = CloudForCustomers(
        url=url,
        endpoint=endpoint,
        report_url=report_url,
        filter_params=filter_params,
        credentials=credentials,
        config_key=config_key,
    )
    # fields=fields, dtype=dtype, tests=tests, 
    return c4c.to_df(**kwargs)
