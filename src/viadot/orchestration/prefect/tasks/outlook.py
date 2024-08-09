"""Task to download data from Outlook API into a Pandas DataFrame."""

import pandas as pd
from prefect import task

from viadot.orchestration.prefect.exceptions import MissingSourceCredentialsError
from viadot.orchestration.prefect.utils import get_credentials
from viadot.sources import Outlook


@task(retries=3, log_prints=True, retry_delay_seconds=10, timeout_seconds=60 * 60)
def outlook_to_df(
    mailbox_name: str,
    config_key: str | None = None,
    azure_key_vault_secret: str | None = None,
    request_retries: int = 10,
    start_date: str | None = None,
    end_date: str | None = None,
    limit: int = 10000,
    address_limit: int = 8000,
    outbox_list: list[str] | None = None,
) -> pd.DataFrame:
    """Task for downloading data from Outlook API to a pandas DataFrame.

    Args:
        mailbox_name (str): Mailbox name.
        config_key (Optional[str], optional): The key in the viadot config holding
            relevant credentials. Defaults to None.
        azure_key_vault_secret (Optional[str], optional): The name of the Azure Key
            Vault secret where credentials are stored. Defaults to None.
        request_retries (int, optional): How many times retries to authorizate.
            Defaults to 10.
        start_date (Optional[str], optional): A filtering start date parameter e.g.
            "2022-01-01". Defaults to None.
        end_date (Optional[str], optional): A filtering end date parameter e.g.
            "2022-01-02". Defaults to None.
        limit (int, optional): Number of fetched top messages. Defaults to 10000.
        address_limit (int, optional): The maximum number of accepted characters in the
            sum of all email names. Defaults to 8000.
        outbox_list (List[str], optional): List of outbox folders to differentiate
            between Inboxes and Outboxes. Defaults to ["Sent Items"].

    Examples:
        data_frame = mindful_to_df(
            config_key=config_key,
            azure_key_vault_secret=azure_key_vault_secret,
            region=region,
            endpoint=end,
            date_interval=date_interval,
            limit=limit,
        )

    Raises:
        MissingSourceCredentialsError: If none credentials have been provided.
        APIError: The mailbox name is a "must" requirement.

    Returns:
        pd.DataFrame: The response data as a pandas DataFrame.
    """
    if not (azure_key_vault_secret or config_key):
        raise MissingSourceCredentialsError

    if not config_key:
        credentials = get_credentials(azure_key_vault_secret)

    if not outbox_list:
        outbox_list = ["Sent Items"]

    outlook = Outlook(
        credentials=credentials,
        config_key=config_key,
    )
    outlook.api_connection(
        mailbox_name=mailbox_name,
        request_retries=request_retries,
        start_date=start_date,
        end_date=end_date,
        limit=limit,
        address_limit=address_limit,
        outbox_list=outbox_list,
    )

    return outlook.to_df()
