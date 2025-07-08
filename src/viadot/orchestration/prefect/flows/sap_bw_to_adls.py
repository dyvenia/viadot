"""Task to download data from SAP BW API into a Pandas DataFrame."""

from typing import Any

from prefect import flow
from prefect.task_runners import ConcurrentTaskRunner

from viadot.orchestration.prefect.tasks import df_to_adls, sap_bw_to_df


@flow(
    name="SAP BW extraction to ADLS",
    description="Extract data from SAP BW and load it into Azure Data Lake Storage.",
    retries=1,
    retry_delay_seconds=60,
    task_runner=ConcurrentTaskRunner,
)
def sap_bw_to_adls(
    config_key: str | None = None,
    azure_key_vault_secret: str | None = None,
    mdx_query: str | None = None,
    mapping_dict: dict[str, Any] | None = None,
    adls_azure_key_vault_secret: str | None = None,
    adls_config_key: str | None = None,
    adls_path: str | None = None,
    adls_path_overwrite: bool = False,
) -> None:
    """Flow for downloading data from SAP BW API to Azure Data Lake.

    Args:
        config_key (Optional[str], optional): The key in the viadot config holding
            relevant credentials. Defaults to None.
        azure_key_vault_secret (Optional[str], optional): The name of the Azure Key
            Vault secret where credentials are stored. Defaults to None.
        mdx_query (str, optional): The MDX query to be passed to connection.
        mapping_dict (dict[str, Any], optional): Dictionary with original and new
            column names. Defaults to None.
        adls_azure_key_vault_secret (str, optional): The name of the Azure Key.
            Defaults to None.
        adls_config_key (str, optional): The key in the viadot config holding relevant
            credentials. Defaults to None.
        adls_path (str, optional): Azure Data Lake destination folder/catalog path.
            Defaults to None.
        adls_path_overwrite (bool, optional): Whether to overwrite the file in ADLS.
            Defaults to False.
    """
    data_frame = sap_bw_to_df(
        config_key=config_key,
        azure_key_vault_secret=azure_key_vault_secret,
        mdx_query=mdx_query,
        mapping_dict=mapping_dict,
    )

    return df_to_adls(
        df=data_frame,
        path=adls_path,
        credentials_secret=adls_azure_key_vault_secret,
        config_key=adls_config_key,
        overwrite=adls_path_overwrite,
    )
