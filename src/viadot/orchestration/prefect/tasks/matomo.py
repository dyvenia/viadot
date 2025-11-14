"""Task for downloading data from Matomo API to a pandas DataFrame."""

from typing import Any, Literal

import pandas as pd
from prefect import task

from viadot.orchestration.prefect.exceptions import MissingSourceCredentialsError
from viadot.orchestration.prefect.utils import get_credentials
from viadot.sources.matomo import Matomo


@task(retries=3, log_prints=True, retry_delay_seconds=10, timeout_seconds=60 * 60)
def matomo_to_df(
    url: str,
    top_level_fields: list[str],
    record_path: str | list[str],
    params: dict[str, Any],
    config_key: str | None = None,
    credentials_secret: str | None = None,
    record_prefix: str | None = None,
    if_empty: Literal["warn", "skip", "fail"] = "warn",
    tests: dict[str, Any] | None = None,
) -> pd.DataFrame:
    """Task to download data from Matomo API to a pandas DataFrame.

    Args:
        url (str): The base URL of the Matomo instance.
        top_level_fields (list[str]): List of top level fields to get from the API JSON.
        record_path (str | list[str]): The path field to the records in the API
            response.Could be handled as a list of path + fields to extract:
                    record_path = 'actionDetails'
                    record_path = ['actionDetails', 'eventAction']
        config_key (Optional[str], optional): The key in the viadot config holding
            relevant credentials. Defaults to None.
        credentials_secret (str, optional): The name of the secret that stores Matomo
            credentials. Defaults to None.
            More info on: https://docs.prefect.io/concepts/blocks/
        params (dict[str, Any]): Parameters for the API request.
                Necessary params and their examples are:
                    "module": "API",
                    "method": "Live.getLastVisitsDetails",
                    "idSite": "53",
                    "period": "range",
                    "date": ("<<yesterday>>", "<<yesterday>>"),
                    "format": "JSON",
        record_prefix (Optional[str], optional): A prefix for the record path fields.
            For example: "action_". Defaults to None.
        if_empty (Literal["warn", "skip", "fail"], optional): What to do if the
            query returns no data. Defaults to "warn".
        tests (dict[str, Any], optional): A dictionary with optional list of tests
            to verify the output dataframe. If defined, triggers the `validate`
            function from viadot.utils. Defaults to None.

    Examples:
        data_frame = matomo_to_df(
            url="https://matomo.example.com",
            top_level_fields=["idSite", "visitorId", "visitIp"],
            record_path="actionDetails",
            record_prefix="action_",
            config_key="matomo_prod",
            params={
                "module": "API",
                "method": "Live.getLastVisitsDetails",
                "idSite": "53",
                "period": "range",
                "date": ("<<yesterday>>", "<<today>>"),
                "format": "JSON"
            },
            record_prefix="action_",
            if_empty="warn"
        )

    Raises:
        MissingSourceCredentialsError: If no credentials have been provided.
        ValueError: If api_token is not found in credentials or if no data
            has been fetched.

    Returns:
        pd.DataFrame: The Matomo data as a pandas DataFrame.
    """
    if not (credentials_secret or config_key):
        raise MissingSourceCredentialsError

    credentials = get_credentials(credentials_secret)
    matomo = Matomo(
        credentials=credentials,
        config_key=config_key,
    )

    params["date"] = matomo.format_date_range(params["date"])

    # Fetch the data using credentials
    data = matomo.fetch_data(
        api_token=matomo.credentials["api_token"],
        url=url,
        params=params,
    )

    # Convert to DataFrame with the specified parameters
    return matomo.to_df(
        data=data,
        top_level_fields=top_level_fields,
        record_path=record_path,
        record_prefix=record_prefix,
        if_empty=if_empty,
        tests=tests,
    )
