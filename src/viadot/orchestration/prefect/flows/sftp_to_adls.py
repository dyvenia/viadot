"""Download data from a SFTP server to Azure Data Lake Storage."""

from prefect import flow
from prefect.task_runners import ConcurrentTaskRunner

from viadot.orchestration.prefect.tasks import df_to_adls, sftp_to_df


@flow(
    name="SFTP extraction to ADLS",
    description="Extract data from a SFTP server and "
    + "load it into Azure Data Lake Storage.",
    retries=1,
    retry_delay_seconds=60,
    task_runner=ConcurrentTaskRunner,
)
def sftp_to_adls(
    config_key: str | None = None,
    azure_key_vault_secret: str | None = None,
    file_name: str | None = None,
    sep: str = "\t",
    columns: list[str] | None = None,
    adls_config_key: str | None = None,
    adls_azure_key_vault_secret: str | None = None,
    adls_path: str | None = None,
    adls_path_overwrite: bool = False,
) -> None:
    r"""Flow to download data from a SFTP server to Azure Data Lake.

    Args:
        config_key (str, optional): The key in the viadot config holding relevant
            credentials. Defaults to None.
        azure_key_vault_secret (str, optional): The name of the Azure Key Vault secret
            where credentials are stored. Defaults to None.
        file_name (str, optional): Path to the file in SFTP server. Defaults to None.
        sep (str, optional): The separator to use to read the CSV file.
            Defaults to "\t".
        columns (List[str], optional): Columns to read from the file. Defaults to None.
        adls_config_key (str, optional): The key in the viadot config holding
            relevant credentials. Defaults to None.
        adls_azure_key_vault_secret (str, optional): The name of the Azure Key
            Vault secret containing a dictionary with ACCOUNT_NAME and Service Principal
            credentials (TENANT_ID, CLIENT_ID, CLIENT_SECRET) for the Azure Data Lake.
            Defaults to None.
        adls_path (str, optional): Azure Data Lake destination file path
            (with file name). Defaults to None.
        adls_path_overwrite (bool, optional): Whether to overwrite the file in ADLS.
            Defaults to True.
    """
    data_frame = sftp_to_df(
        config_key=config_key,
        azure_key_vault_secret=azure_key_vault_secret,
        file_name=file_name,
        sep=sep,
        columns=columns,
    )

    return df_to_adls(
        df=data_frame,
        path=adls_path,
        credentials_secret=adls_azure_key_vault_secret,
        config_key=adls_config_key,
        overwrite=adls_path_overwrite,
    )
