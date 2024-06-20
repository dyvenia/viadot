import logging
from typing import Any, Dict, List, Optional

import pandas as pd
from prefect import get_run_logger, task

from viadot.exceptions import APIError
from viadot.orchestration.prefect.exceptions import MissingSourceCredentialsError
from viadot.orchestration.prefect.utils import get_credentials
from viadot.sources import Genesys

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


@task(retries=3, retry_delay_seconds=10, timeout_seconds=2 * 60 * 60)
def genesys_to_df(
    credentials: Optional[Dict[str, Any]] = None,
    config_key: str = None,
    azure_key_vault_secret: Optional[str] = None,
    verbose: Optional[bool] = None,
    endpoint: Optional[str] = None,
    environment: str = "mypurecloud.de",
    queues_ids: Optional[List[str]] = None,
    view_type: Optional[str] = None,
    view_type_time_sleep: Optional[int] = None,
    post_data_list: Optional[List[Dict[str, Any]]] = None,
    normalization_sep: str = ".",
    drop_duplicates: bool = False,
    validate_df_dict: Optional[Dict[str, Any]] = None,
) -> pd.DataFrame:
    """
    Description:
        Task for downloading data from Genesys API.

    Args:
        credentials (Optional[Dict[str, Any]], optional): Genesys credentials as a dictionary.
            Defaults to None.
        config_key (str, optional): The key in the viadot config holding relevant credentials.
            Defaults to None.
        azure_key_vault_secret (Optional[str], optional): The name of the Azure Key Vault secret
            where credentials are stored. Defaults to None.
        verbose (bool, optional): Increase the details of the logs printed on the screen.
                Defaults to False.
        endpoint (Optional[str], optional): Final end point to the API. Defaults to None.
        environment (str, optional): the domain that appears for Genesys Cloud Environment
            based on the location of your Genesys Cloud organization. Defaults to "mypurecloud.de".
        queues_ids (Optional[List[str]], optional): List of queues ids to consult the
                members. Defaults to None.
        view_type (Optional[str], optional): The type of view export job to be created.
            Defaults to None.
        view_type_time_sleep (Optional[int], optional): Waiting time to retrieve data from Genesys
            Cloud API. Defaults to None.
        post_data_list (Optional[List[Dict[str, Any]]], optional): List of string templates to generate
            json body in POST calls to the API. Defaults to None.
        normalization_sep (str, optional): Nested records will generate names separated by sep.
            Defaults to ".".
        drop_duplicates (bool, optional): Remove duplicates from the Data Frame. Defaults to False.
        validate_df_dict (Optional[Dict[str, Any]], optional): A dictionary with
            optional list of tests to verify the output dataframe. Defaults to None.

    Returns:
        pd.DataFrame: The response data as a Pandas Data Frame.
    """

    logger = get_run_logger()

    if not (azure_key_vault_secret or config_key or credentials):
        raise MissingSourceCredentialsError

    if not config_key:
        credentials = credentials or get_credentials(azure_key_vault_secret)

    if endpoint is None:
        raise APIError("The API endpoint parameter was not defined.")

    genesys = Genesys(
        credentials=credentials,
        config_key=config_key,
        verbose=verbose,
        environment=environment,
    )
    genesys.api_connection(
        endpoint=endpoint,
        queues_ids=queues_ids,
        view_type=view_type,
        view_type_time_sleep=view_type_time_sleep,
        post_data_list=post_data_list,
        normalization_sep=normalization_sep,
    )
    data_frame = genesys.to_df(
        drop_duplicates=drop_duplicates,
        validate_df_dict=validate_df_dict,
    )

    return data_frame
