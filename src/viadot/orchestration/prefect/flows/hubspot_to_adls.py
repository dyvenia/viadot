"""Download data from Hubspot API and load into Azure Data Lake Storage."""

from typing import Any

from prefect import flow
from prefect.task_runners import ConcurrentTaskRunner

from viadot.orchestration.prefect.tasks import df_to_adls, hubspot_to_df


@flow(
    name="Hubspot extraction to ADLS",
    description="Extract data from Hubspot API and load into Azure Data Lake Storage.",
    retries=1,
    retry_delay_seconds=60,
    task_runner=ConcurrentTaskRunner,
)
def hubspot_to_adls(
    config_key: str | None = None,
    azure_key_vault_secret: str | None = None,
    endpoint: str | None = None,
    filters: list[dict[str, Any]] | None = None,
    properties: list[Any] | None = None,
    nrows: int = 1000,
    adls_config_key: str | None = None,
    adls_azure_key_vault_secret: str | None = None,
    adls_path: str | None = None,
    adls_path_overwrite: bool = False,
) -> None:
    """Flow for downloading data from mindful to Azure Data Lake.

    Args:
        config_key (str, optional): The key in the viadot config holding relevant
            credentials. Defaults to None.
        azure_key_vault_secret (Optional[str], optional): The name of the Azure Key
            Vault secret where credentials are stored. Defaults to None.
        endpoint (Optional[str], optional): API endpoint for an individual request.
            Defaults to None.
        filters (Optional[List[Dict[str, Any]]], optional): Filters defined for the API
            body in specific order. Defaults to None.
        properties (Optional[List[Any]], optional): List of user-defined columns to be
            pulled from the API. Defaults to None.
        nrows (int, optional): Max number of rows to pull during execution.
            Defaults to 1000.
        adls_config_key (Optional[str], optional): The key in the viadot config holding
            relevant credentials. Defaults to None.
        adls_azure_key_vault_secret (Optional[str], optional): The name of the Azure Key
            Vault secret containing a dictionary with ACCOUNT_NAME and Service Principal
            credentials (TENANT_ID, CLIENT_ID, CLIENT_SECRET) for the Azure Data Lake.
            Defaults to None.
        adls_path (Optional[str], optional): Azure Data Lake destination file path
            (with file name). Defaults to None.
        adls_path_overwrite (bool, optional): Whether to overwrite the file in ADLS.
            Defaults to True.

    Examples:
        hubspot_to_adls(
            config_key=config_key,
            endpoint=endpoint,
            nrows=nrows,
            adls_config_key=adls_config_key,
            adls_path=adls_path,
            adls_path_overwrite=True,
        )
    """
    data_frame = hubspot_to_df(
        config_key=config_key,
        azure_key_vault_secret=azure_key_vault_secret,
        endpoint=endpoint,
        filters=filters,
        properties=properties,
        nrows=nrows,
    )

    return df_to_adls(
        df=data_frame,
        path=adls_path,
        credentials_secret=adls_azure_key_vault_secret,
        config_key=adls_config_key,
        overwrite=adls_path_overwrite,
    )
