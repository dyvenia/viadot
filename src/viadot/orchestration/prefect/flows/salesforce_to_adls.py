"""Download data from Salesforce API to Azure Data Lake Storage."""

from prefect import flow
from prefect.task_runners import ConcurrentTaskRunner

from viadot.orchestration.prefect.tasks import df_to_adls, salesforce_to_df


@flow(
    name="Salesforce extraction to ADLS",
    description="Extract data from Salesforce and load "
    + "it into Azure Data Lake Storage.",
    retries=1,
    retry_delay_seconds=60,
    task_runner=ConcurrentTaskRunner,
)
def salesforce_to_adls(  # noqa: PLR0913
    config_key: str | None = None,
    azure_key_vault_secret: str | None = None,
    env: str | None = None,
    domain: str | None = None,
    client_id: str | None = None,
    query: str | None = None,
    table: str | None = None,
    columns: list[str] | None = None,
    adls_config_key: str | None = None,
    adls_azure_key_vault_secret: str | None = None,
    adls_path: str | None = None,
    adls_path_overwrite: bool = False,
) -> None:
    """Flow to download data from Salesforce API to Azure Data Lake.

    Args:
        config_key (str, optional): The key in the viadot config holding relevant
            credentials. Defaults to None.
        azure_key_vault_secret (str, optional): The name of the Azure Key Vault secret
            where credentials are stored. Defaults to None.
        env (str, optional): Environment information, provides information about
            credential and connection configuration. Defaults to 'DEV'.
        domain (str, optional): Domain of a connection. defaults to 'test' (sandbox).
            Can only be added if built-in username/password/security token is provided.
            Defaults to None.
        client_id (str, optional): Client id to keep the track of API calls.
            Defaults to None.
        query (str, optional): Query for download the data if specific download is
            needed. Defaults to None.
        table (str, optional): Table name. Can be used instead of query.
            Defaults to None.
        columns (list[str], optional): List of columns which are needed - table
            argument is needed. Defaults to None.
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
    data_frame = salesforce_to_df(
        config_key=config_key,
        azure_key_vault_secret=azure_key_vault_secret,
        env=env,
        domain=domain,
        client_id=client_id,
        query=query,
        table=table,
        columns=columns,
    )

    return df_to_adls(
        df=data_frame,
        path=adls_path,
        credentials_secret=adls_azure_key_vault_secret,
        config_key=adls_config_key,
        overwrite=adls_path_overwrite,
    )
