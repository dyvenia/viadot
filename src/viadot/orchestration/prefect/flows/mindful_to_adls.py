import os
import time
from datetime import date
from typing import Any, Dict, List, Literal, Optional, Union

from prefect import flow
from prefect.task_runners import ConcurrentTaskRunner

from viadot.orchestration.prefect.tasks import df_to_adls, mindful_to_df


@flow(
    name="Mindful extraction to ADLS",
    description="Extract data from mindful and load it into Azure Data Lake Storage.",
    retries=1,
    retry_delay_seconds=60,
    task_runner=ConcurrentTaskRunner,
)
def mindful_to_adls(
    credentials: Optional[Dict[str, Any]] = None,
    config_key: str = "mindful",
    azure_key_vault_secret: Optional[str] = None,
    region: Literal["us1", "us2", "us3", "ca1", "eu1", "au1"] = "eu1",
    endpoint: Optional[Union[List[str], str]] = None,
    date_interval: Optional[List[date]] = None,
    limit: int = 1000,
    adls_credentials: Optional[Dict[str, Any]] = None,
    adls_config_key: Optional[str] = None,
    adls_azure_key_vault_secret: Optional[str] = None,
    adls_path: Optional[str] = None,
    adls_path_overwrite: bool = False,
):
    """
    Description:
        Flow for downloading data from mindful to Azure Data Lake.

    Args:
        credentials (Optional[Dict[str, Any]], optional): Mindful credentials as a dictionary.
            Defaults to None.
        config_key (str, optional): The key in the viadot config holding relevant credentials.
            Defaults to "mindful".
        azure_key_vault_secret (Optional[str], optional): The name of the Azure Key Vault secret
            where credentials are stored. Defaults to None.
        region (Literal[us1, us2, us3, ca1, eu1, au1], optional): Survey Dynamix region from
            where to interact with the mindful API. Defaults to "eu1" English (United Kingdom).
        endpoint (Optional[Union[List[str], str]], optional): Endpoint name or list of them from
            where to download data. Defaults to None.
        date_interval (Optional[List[date]], optional): Date time range detailing the starting date and the ending date.
            If no range is passed, one day of data since this moment will be retrieved. Defaults to None.
        limit (int, optional): The number of matching interactions to return. Defaults to 1000.
        adls_credentials (Optional[Dict[str, Any]], optional): The credentials as a dictionary.
            Defaults to None.
        adls_config_key (Optional[str], optional): The key in the viadot config holding
            relevant credentials. Defaults to None.
        adls_azure_key_vault_secret (Optional[str], optional): The name of the Azure Key Vault secret containing
            a dictionary with ACCOUNT_NAME and Service Principal credentials (TENANT_ID, CLIENT_ID, CLIENT_SECRET)
            for the Azure Data Lake. Defaults to None.
        adls_path (Optional[str], optional): Azure Data Lake destination file path. Defaults to None.
        adls_path_overwrite (bool, optional): Whether to overwrite the file in ADLS. Defaults to True.
    """

    if isinstance(endpoint, str):
        endpoint = [endpoint]

    for end in endpoint:
        data_frame = mindful_to_df(
            credentials=credentials,
            config_key=config_key,
            azure_key_vault_secret=azure_key_vault_secret,
            region=region,
            endpoint=end,
            date_interval=date_interval,
            limit=limit,
        )
        time.sleep(0.5)

        df_to_adls(
            df=data_frame,
            path=os.path.join(adls_path, f"{end}.csv"),
            credentials=adls_credentials,
            credentials_secret=adls_azure_key_vault_secret,
            config_key=adls_config_key,
            overwrite=adls_path_overwrite,
        )
