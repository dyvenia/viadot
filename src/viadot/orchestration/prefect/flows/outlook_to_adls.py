"""Download data from Outlook API to Azure Data Lake Storage."""

from prefect import flow
from prefect.task_runners import ConcurrentTaskRunner

from viadot.orchestration.prefect.tasks import df_to_adls, outlook_to_df


@flow(
    name="Outlook extraction to ADLS",
    description="Extract data from Outlook and load it into Azure Data Lake Storage.",
    retries=1,
    retry_delay_seconds=60,
    task_runner=ConcurrentTaskRunner,
)
def outlook_to_adls(  # noqa: PLR0913
    config_key: str | None = None,
    azure_key_vault_secret: str | None = None,
    mailbox_name: str | None = None,
    request_retries: int = 10,
    start_date: str | None = None,
    end_date: str | None = None,
    limit: int = 10000,
    address_limit: int = 8000,
    outbox_list: list[str] | None = None,
    adls_config_key: str | None = None,
    adls_azure_key_vault_secret: str | None = None,
    adls_path: str | None = None,
    adls_path_overwrite: bool = False,
) -> None:
    """Flow to download data from Outlook API to Azure Data Lake.

    Args:
        credentials (Optional[Dict[str, Any]], optional): Outlook credentials as a
            dictionary. Defaults to None.
        config_key (Optional[str], optional): The key in the viadot config holding
            relevant credentials. Defaults to None.
        azure_key_vault_secret (Optional[str], optional): The name of the Azure Key
            Vault secret where credentials are stored. Defaults to None.
        mailbox_name (Optional[str], optional): Mailbox name. Defaults to None.
        request_retries (int, optional): How many times to retry the connection to
            Outlook. Defaults to 10.
        start_date (Optional[str], optional): A filtering start date parameter e.g.
            "2022-01-01". Defaults to None.
        end_date (Optional[str], optional): A filtering end date parameter e.g.
            "2022-01-02". Defaults to None.
        limit (int, optional): Number of fetched top messages. Defaults to 10000.
        address_limit (int, optional): The maximum number of accepted characters in the
            sum of all email names. Defaults to 8000.
        outbox_list (List[str], optional): List of outbox folders to differentiate
            between Inboxes and Outboxes. Defaults to ["Sent Items"].
        adls_credentials (Optional[Dict[str, Any]], optional): The credentials as a
            dictionary. Defaults to None.
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
        outlook_to_adls(
            config_key=config_key,
            mailbox_name=mailbox_name,
            start_date=start_date,
            end_date=end_date,
            adls_config_key=adls_config_key,
            adls_path=adls_path,
            adls_path_overwrite=True,
        )
    """
    if outbox_list is None:
        outbox_list = ["Sent Items"]

    data_frame = outlook_to_df(
        config_key=config_key,
        azure_key_vault_secret=azure_key_vault_secret,
        mailbox_name=mailbox_name,
        request_retries=request_retries,
        start_date=start_date,
        end_date=end_date,
        limit=limit,
        address_limit=address_limit,
        outbox_list=outbox_list,
    )

    return df_to_adls(
        df=data_frame,
        path=adls_path,
        credentials_secret=adls_azure_key_vault_secret,
        config_key=adls_config_key,
        overwrite=adls_path_overwrite,
    )
