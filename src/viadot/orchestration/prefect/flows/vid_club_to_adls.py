"""Download data from Vid CLub API and load it into Azure Data Lake Storage."""

from typing import Any, Literal

from prefect import flow
from prefect.task_runners import ConcurrentTaskRunner

from viadot.orchestration.prefect.tasks import df_to_adls, vid_club_to_df


@flow(
    name="Vid CLub extraction to ADLS",
    description="Extract data from Vid CLub and load it into Azure Data Lake Storage.",
    retries=1,
    retry_delay_seconds=60,
    task_runner=ConcurrentTaskRunner,
)
def vid_club_to_adls(  # noqa: PLR0913
    *args: list[Any],
    endpoint: Literal["jobs", "product", "company", "survey"] | None = None,
    from_date: str = "2022-03-22",
    to_date: str | None = None,
    items_per_page: int = 100,
    region: Literal["bg", "hu", "hr", "pl", "ro", "si", "all"] | None = None,
    days_interval: int = 30,
    cols_to_drop: list[str] | None = None,
    config_key: str | None = None,
    azure_key_vault_secret: str | None = None,
    adls_config_key: str | None = None,
    adls_azure_key_vault_secret: str | None = None,
    adls_path: str | None = None,
    adls_path_overwrite: bool = False,
    validate_df_dict: dict | None = None,
    timeout: int = 3600,
    **kwargs: dict[str, Any],
) -> None:
    """Flow for downloading data from the Vid Club via API to a CSV or Parquet file.

    Then upload it to Azure Data Lake.

    Args:
        endpoint (Literal["jobs", "product", "company", "survey"], optional): The
        endpoint source to be accessed. Defaults to None.
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
        adls_config_key (Optional[str], optional): The key in the viadot config holding
            relevant credentials. Defaults to None.
        adls_azure_key_vault_secret (Optional[str], optional): The name of the Azure Key
            Vault secret containing a dictionary with ACCOUNT_NAME and Service Principal
            credentials (TENANT_ID, CLIENT_ID, CLIENT_SECRET) for the Azure Data Lake.
            Defaults to None.
        adls_path (Optional[str], optional): Azure Data Lake destination file path.
            Defaults to None.
        adls_path_overwrite (bool, optional): Whether to overwrite the file in ADLS.
            Defaults to True.
        validate_df_dict (dict, optional): A dictionary with optional list of tests
            to verify the output
            dataframe. If defined, triggers the `validate_df` task from task_utils.
            Defaults to None.
        timeout (int, optional): The time (in seconds) to wait while running this task
            before a timeout occurs. Defaults to 3600.
    """
    data_frame = vid_club_to_df(
        args=args,
        endpoint=endpoint,
        from_date=from_date,
        to_date=to_date,
        items_per_page=items_per_page,
        region=region,
        days_interval=days_interval,
        cols_to_drop=cols_to_drop,
        config_key=config_key,
        azure_key_vault_secret=azure_key_vault_secret,
        validate_df_dict=validate_df_dict,
        timeout=timeout,
        kawrgs=kwargs,
    )

    return df_to_adls(
        df=data_frame,
        path=adls_path,
        credentials_secret=adls_azure_key_vault_secret,
        config_key=adls_config_key,
        overwrite=adls_path_overwrite,
    )
