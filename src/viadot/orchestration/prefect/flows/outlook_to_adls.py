import time
from typing import Any, Dict, List, Optional

from prefect import flow
from prefect.task_runners import ConcurrentTaskRunner

from viadot.orchestration.prefect.tasks import df_to_adls, outlook_to_df


@flow(
    name="Outlook extraction to ADLS",
    description="Extract data from Outlook O365 and load it into Azure Data Lake Storage.",
    retries=1,
    retry_delay_seconds=60,
    task_runner=ConcurrentTaskRunner,
)
def outlook_to_adls(
    credentials: Optional[Dict[str, Any]] = None,
    config_key: str = None,
    azure_key_vault_secret: Optional[str] = None,
    mailbox_name: Optional[str] = None,
):
    data_frame = outlook_to_df(
        credentials=credentials,
        config_key=config_key,
        azure_key_vault_secret=azure_key_vault_secret,
        mailbox_name=mailbox_name,
    )
