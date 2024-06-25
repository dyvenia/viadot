import logging
from typing import Any, Dict, List, Optional

import pandas as pd
from prefect import get_run_logger, task

from viadot.exceptions import APIError
from viadot.orchestration.prefect.exceptions import MissingSourceCredentialsError
from viadot.orchestration.prefect.utils import get_credentials
from viadot.sources import Outlook

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


@task(retries=3, retry_delay_seconds=10, timeout_seconds=60 * 60)
def outlook_to_df(
    credentials: Optional[Dict[str, Any]] = None,
    config_key: Optional[str] = None,
    azure_key_vault_secret: Optional[str] = None,
    mailbox_name: Optional[str] = None,
) -> pd.DataFrame:
    logger = get_run_logger()

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
        start_date="2023-04-12",
        end_date="2023-04-13",
    )
