"""Task for downloading data from Genesys Cloud API."""

from typing import Any

import pandas as pd
from prefect import get_run_logger, task

from viadot.exceptions import APIError
from viadot.orchestration.prefect.exceptions import MissingSourceCredentialsError
from viadot.orchestration.prefect.utils import get_credentials
from viadot.sources import Genesys


@task(retries=3, log_prints=True, retry_delay_seconds=10, timeout_seconds=2 * 60 * 60)
def genesys_to_df(  # noqa: PLR0913
    config_key: str | None = None,
    azure_key_vault_secret: str | None = None,
    verbose: bool | None = None,
    endpoint: str | None = None,
    environment: str = "mypurecloud.de",
    queues_ids: list[str] | None = None,
    view_type: str | None = None,
    view_type_time_sleep: int | None = None,
    post_data_list: list[dict[str, Any]] | None = None,
    time_between_api_call: float = 0.5,
    normalization_sep: str = ".",
    drop_duplicates: bool = False,
    validate_df_dict: dict[str, Any] | None = None,
) -> pd.DataFrame:
    """Task to download data from Genesys Cloud API.

    Args:
        config_key (str, optional): The key in the viadot config holding relevant
            credentials. Defaults to None.
        azure_key_vault_secret (Optional[str], optional): The name of the Azure Key
            Vault secret where credentials are stored. Defaults to None.
        verbose (bool, optional): Increase the details of the logs printed on the
                screen. Defaults to False.
        endpoint (Optional[str], optional): Final end point to the API.
            Defaults to None.
        environment (str, optional): the domain that appears for Genesys Cloud
            Environment based on the location of your Genesys Cloud organization.
            Defaults to "mypurecloud.de".
        queues_ids (Optional[List[str]], optional): List of queues ids to consult the
                members. Defaults to None.
        view_type (Optional[str], optional): The type of view export job to be created.
            Defaults to None.
        view_type_time_sleep (Optional[int], optional): Waiting time to retrieve data
            from Genesys Cloud API. Defaults to None.
        post_data_list (Optional[List[Dict[str, Any]]], optional): List of string
            templates to generate json body in POST calls to the API. Defaults to None.
        time_between_api_call (int, optional): The time, in seconds, to sleep the call
            to the API. Defaults to 0.5.
        normalization_sep (str, optional): Nested records will generate names separated
            by sep. Defaults to ".".
        drop_duplicates (bool, optional): Remove duplicates from the DataFrame.
            Defaults to False.
        validate_df_dict (Optional[Dict[str, Any]], optional): A dictionary with
            optional list of tests to verify the output dataframe. Defaults to None.

    Examples:
        data_frame = genesys_to_df(
            config_key=config_key,
            azure_key_vault_secret=azure_key_vault_secret,
            verbose=verbose,
            endpoint=endpoint,
            environment=environment,
            queues_ids=queues_ids,
            view_type=view_type,
            view_type_time_sleep=view_type_time_sleep,
            post_data_list=post_data_list,
            normalization_sep=normalization_sep,
            validate_df_dict=validate_df_dict,
        )

    Returns:
        pd.DataFrame: The response data as a pandas DataFrame.
    """
    logger = get_run_logger()

    if not (azure_key_vault_secret or config_key):
        raise MissingSourceCredentialsError

    if not config_key:
        credentials = get_credentials(azure_key_vault_secret)

    if endpoint is None:
        msg = "The API endpoint parameter was not defined."
        raise APIError(msg)

    genesys = Genesys(
        credentials=credentials,
        config_key=config_key,
        verbose=verbose,
        environment=environment,
    )
    logger.info("running `api_connection` method:\n")
    genesys.api_connection(
        endpoint=endpoint,
        queues_ids=queues_ids,
        view_type=view_type,
        view_type_time_sleep=view_type_time_sleep,
        post_data_list=post_data_list,
        time_between_api_call=time_between_api_call,
        normalization_sep=normalization_sep,
    )
    logger.info("running `to_df` method:\n")

    return genesys.to_df(
        drop_duplicates=drop_duplicates,
        validate_df_dict=validate_df_dict,
    )
