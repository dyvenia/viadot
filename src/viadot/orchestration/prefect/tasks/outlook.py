"""
'mindful.py'.

Prefect task wrapper for the Mindful API connector.

This module provides an intermediate wrapper between the prefect flow and the connector:
- Generate the Mindful API connector.
- Create and return a pandas Data Frame with the response of the API.

Typical usage example:

    data_frame = mindful_to_df(
        credentials=credentials,
        config_key=config_key,
        azure_key_vault_secret=azure_key_vault_secret,
        region=region,
        endpoint=end,
        date_interval=date_interval,
        limit=limit,
    )

Functions:

    mindful_to_df(credentials, config_key, azure_key_vault_secret, region,
        endpoint, date_interval, limit): Task to download data from Mindful API.
"""  # noqa: D412

from typing import Any, Dict, List, Optional

import pandas as pd
from prefect import task

from viadot.exceptions import APIError
from viadot.orchestration.prefect.exceptions import MissingSourceCredentialsError
from viadot.orchestration.prefect.utils import get_credentials
from viadot.sources import Outlook


@task(retries=3, log_prints=True, retry_delay_seconds=10, timeout_seconds=60 * 60)
def outlook_to_df(
    credentials: Optional[Dict[str, Any]] = None,
    config_key: Optional[str] = None,
    azure_key_vault_secret: Optional[str] = None,
    mailbox_name: Optional[str] = None,
    request_retries: int = 10,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    limit: int = 10000,
    address_limit: int = 8000,
    outbox_list: List[str] = ["Sent Items"],
) -> pd.DataFrame:
    """
    Task for downloading data from Outlook API to Data Frame.

    Args:
        credentials (Optional[Dict[str, Any]], optional): Outlook credentials as a
            dictionary. Defaults to None.
        config_key (Optional[str], optional): The key in the viadot config holding
            relevant credentials. Defaults to None.
        azure_key_vault_secret (Optional[str], optional): The name of the Azure Key
            Vault secret where credentials are stored. Defaults to None.
        mailbox_name (Optional[str], optional): Mailbox name. Defaults to None.
        request_retries (int, optional): How many times retries to authorizate.
            Defaults to 10.
        start_date (Optional[str], optional): A filtering start date parameter e.g.
            "2022-01-01". Defaults to None.
        end_date (Optional[str], optional): A filtering end date parameter e.g.
            "2022-01-02". Defaults to None.
        limit (int, optional): Number of fetched top messages. Defaults to 10000.
        address_limit (int, optional): The maximum number of accepted characters in the
            sum of all email names. Defaults to 8000.
        outbox_list (List[str], optional): List of outbox folders to differenciate
            between Inboxes and Outboxes. Defaults to ["Sent Items"].

    Raises:
        MissingSourceCredentialsError: If none credentials have been provided.
        APIError: The mailbox name is a "must" requirement.

    Returns:
        pd.DataFrame: The response data as a Pandas Data Frame.
    """
    if not (azure_key_vault_secret or config_key or credentials):
        raise MissingSourceCredentialsError

    if not config_key:
        credentials = credentials or get_credentials(azure_key_vault_secret)

    if mailbox_name is None:
        raise APIError("Outlook mailbox name is a mandatory requirement.")

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
    data_frame = outlook.to_df()

    return data_frame
