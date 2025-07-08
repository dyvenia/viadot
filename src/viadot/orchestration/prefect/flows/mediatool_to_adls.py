"""'mediatool_to_adls.py'."""

from typing import Any

from prefect import flow
from prefect.task_runners import ConcurrentTaskRunner

from viadot.orchestration.prefect.tasks import df_to_adls, mediatool_to_df


@flow(
    name="Mediatool extraction to ADLS",
    description="Extract data from Mediatool and load it into Azure Data Lake Storage.",
    retries=1,
    retry_delay_seconds=60,
    task_runner=ConcurrentTaskRunner,
)
def mediatool_to_adls(
    config_key: str | None = None,
    azure_key_vault_secret: str | None = None,
    organization_ids: list[str] | None = None,
    media_entries_columns: list[str] | None = None,
    adls_credentials: dict[str, Any] | None = None,
    adls_config_key: str | None = None,
    adls_azure_key_vault_secret: str | None = None,
    adls_path: str | None = None,
    adls_path_overwrite: bool = False,
) -> None:
    """Download data from Mediatool to Azure Data Lake.

    Args:
        config_key (str, optional): The key in the viadot config holding relevant
            credentials. Defaults to None.
        azure_key_vault_secret (str, optional): The name of the Azure Key Vault secret
            where credentials are stored. Defaults to None.
        organization_ids (list[str], optional): List of organization IDs.
            Defaults to None.
        media_entries_columns (list[str], optional): Columns to get from media entries.
            Defaults to None.
        adls_credentials (dict[str, Any], optional): The credentials as a dictionary.
            Defaults to None.
        adls_config_key (str, optional): The key in the viadot config holding relevant
            credentials. Defaults to None.
        adls_azure_key_vault_secret (str, optional): The name of the Azure Key Vault
            secret containing a dictionary with ACCOUNT_NAME and Service Principal
            credentials (TENANT_ID, CLIENT_ID, CLIENT_SECRET) for the Azure Data Lake.
            Defaults to None.
        adls_path (str, optional): Azure Data Lake destination file path.
            Defaults to None.
        adls_path_overwrite (bool, optional): Whether to overwrite the file in ADLS.
            Defaults to True.
    """
    data_frame = mediatool_to_df(
        config_key=config_key,
        azure_key_vault_secret=azure_key_vault_secret,
        organization_ids=organization_ids,
        media_entries_columns=media_entries_columns,
    )

    return df_to_adls(
        df=data_frame,
        path=adls_path,
        credentials=adls_credentials,
        credentials_secret=adls_azure_key_vault_secret,
        config_key=adls_config_key,
        overwrite=adls_path_overwrite,
    )
