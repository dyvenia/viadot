"""Task for downloading data from Vid Club Cloud API."""

from typing import Any, Dict, List, Literal
import pandas as pd
from prefect import task
from viadot.sources import VidClub
from viadot.orchestration.prefect.utils import get_credentials
from viadot.orchestration.prefect.exceptions import MissingSourceCredentialsError


@task(retries=3, log_prints=True, retry_delay_seconds=10, timeout_seconds=2 * 60 * 60)
def vid_club_to_df(
    *args: List[Any],
    endpoint: Literal["jobs", "product", "company", "survey"] = None,
    from_date: str = "2022-03-22",
    to_date: str = None,
    items_per_page: int = 100,
    region: Literal["bg", "hu", "hr", "pl", "ro", "si", "all"] = None,
    days_interval: int = 30,
    cols_to_drop: List[str] = None,
    vid_club_credentials: Dict[str, Any] = None,
    vidclub_credentials_secret: str = "VIDCLUB",
    validate_df_dict: dict = None,
    timeout: int = 3600,
    **kwargs: Dict[str, Any],
) -> pd.DataFrame:
    """
    Task to downloading data from Vid Club APIs to Pandas DataFrame.

    Args:
        endpoint (Literal["jobs", "product", "company", "survey"], optional): The endpoint
            source to be accessed. Defaults to None.
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
        vid_club_credentials (Dict[str, Any], optional): Stores the credentials
            information. Defaults to None.
        vidclub_credentials_secret (str, optional): The name of the secret in
            Azure Key Vault or Prefect or local_config file. Defaults to "VIDCLUB".
        validate_df_dict (dict, optional): A dictionary with optional list of tests
            to verify the output
            dataframe. If defined, triggers the `validate_df` task from task_utils.
            Defaults to None.
        timeout (int, optional): The time (in seconds) to wait while running this task
            before a timeout occurs. Defaults to 3600.

    Returns: Pandas DataFrame
    """
    if not vid_club_credentials:
        vid_club_credentials = get_credentials(vidclub_credentials_secret)

    if not vid_club_credentials:
        raise MissingSourceCredentialsError

    vc_obj = VidClub(
        args=args,
        endpoint=endpoint,
        from_date=from_date,
        to_date=to_date,
        items_per_page=items_per_page,
        region=region,
        days_interval=days_interval,
        cols_to_drop=cols_to_drop,
        vid_club_credentials=vid_club_credentials,
        validate_df_dict=validate_df_dict,
        timeout=timeout,
        kwargs=kwargs
                )

    vc_dataframe = vc_obj.to_df()

    return vc_dataframe
