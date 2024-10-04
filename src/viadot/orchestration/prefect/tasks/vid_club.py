"""Task for downloading data from Vid Club Cloud API."""

from typing import Any, Literal

import pandas as pd
from prefect import task

from viadot.orchestration.prefect.exceptions import MissingSourceCredentialsError
from viadot.orchestration.prefect.utils import get_credentials
from viadot.sources import VidClub


@task(retries=3, log_prints=True, retry_delay_seconds=10, timeout_seconds=2 * 60 * 60)
def vid_club_to_df(  # noqa: PLR0913
    *args: list[Any],
    endpoint: Literal["jobs", "product", "company", "survey"] | None = None,
    from_date: str = "2022-03-22",
    to_date: str | None = None,
    items_per_page: int = 100,
    region: Literal["bg", "hu", "hr", "pl", "ro", "si", "all"] | None = None,
    days_interval: int = 30,
    cols_to_drop: list[str] | None = None,
    azure_key_vault_secret: str | None = None,
    adls_config_key: str | None = None,
    validate_df_dict: dict | None = None,
    timeout: int = 3600,
    **kwargs: dict[str, Any],
) -> pd.DataFrame:
    """Task to downloading data from Vid Club APIs to Pandas DataFrame.

    Args:
        endpoint (Literal["jobs", "product", "company", "survey"], optional):
            The endpoint source to be accessed. Defaults to None.
        from_date (str, optional): Start date for the query, by default is the oldest
            date in the data 2022-03-22.
        to_date (str, optional): End date for the query. By default None,
            which will be executed as datetime.today().strftime("%Y-%m-%d") in code.
        items_per_page (int, optional): Number of entries per page. Defaults to 100.
        region (Literal["bg", "hu", "hr", "pl", "ro", "si", "all"], optional): Region
            filter for the query. Defaults to None (parameter is not used in url).
            [December 2023 status: value 'all' does not work for company and jobs]
        days_interval (int, optional): Days specified in date range per API call
            (test showed that 30-40 is optimal for performance). Defaults to 30.
        cols_to_drop (List[str], optional): List of columns to drop. Defaults to None.
        config_key (str, optional): The key in the viadot config holding relevant
            credentials. Defaults to None.
        azure_key_vault_secret (Optional[str], optional): The name of the Azure Key
            Vault secret where credentials are stored. Defaults to None.
        validate_df_dict (dict, optional): A dictionary with optional list of tests
            to verify the output
            dataframe. If defined, triggers the `validate_df` task from task_utils.
            Defaults to None.
        timeout (int, optional): The time (in seconds) to wait while running this task
            before a timeout occurs. Defaults to 3600.

    Returns: Pandas DataFrame
    """
    if not (azure_key_vault_secret or adls_config_key):
        raise MissingSourceCredentialsError

    if not adls_config_key:
        credentials = get_credentials(azure_key_vault_secret)

    vc_obj = VidClub(
        args=args,
        endpoint=endpoint,
        from_date=from_date,
        to_date=to_date,
        items_per_page=items_per_page,
        region=region,
        days_interval=days_interval,
        cols_to_drop=cols_to_drop,
        vid_club_credentials=credentials,
        validate_df_dict=validate_df_dict,
        timeout=timeout,
        kwargs=kwargs,
    )

    return vc_obj.to_df()
