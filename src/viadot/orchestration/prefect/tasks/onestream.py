"""Tasks for OneStream API."""

from itertools import product
from typing import Any, Literal

import pandas as pd
from prefect import task
import requests

from viadot.orchestration.prefect.exceptions import MissingSourceCredentialsError
from viadot.orchestration.prefect.utils import get_credentials
from viadot.sources.onestream import OneStream


@task(retries=1, log_prints=True, retry_delay_seconds=5)
def create_batch_list_of_custom_subst_vars(
    custom_subst_vars: dict[str, list[Any]],
) -> list[dict[str, list[Any]]]:
    """Generate all combinations of custom substitution variables as batch dictionaries.

    Each combination is used as a separate batch when batch_by_subst_vars is True,
    allowing for individual processing and storage of parquet files in S3.

    Args:
        custom_subst_vars (dict[str, list[Any]]): Dictionary where each key maps to a
        list of possible values for that substitution variable. The cartesian product
            of these lists will be computed.

    Returns:
        list[dict[str, list[Any]]]: List of dictionaries for substitution
            variables, each representing a unique combination of custom variable values.
            Each dictionary has the same keys as the input but with single values
                selected from the corresponding lists.

    Raises:
        ValueError: If custom_subst_vars is empty or contains empty lists.
    """
    if not custom_subst_vars:
        msg = "custom_subst_vars cannot be empty"
        raise ValueError(msg)

    if any(not values for values in custom_subst_vars.values()):
        msg = "All substitution variable lists must contain at least one value"
        raise ValueError(msg)

    return [
        dict(
            zip(
                custom_subst_vars.keys(),
                [[value] for value in combination],
                strict=False,
            )
        )
        for combination in product(*custom_subst_vars.values())
    ]


@task(retries=3, log_prints=True, retry_delay_seconds=10, timeout_seconds=60 * 60)
def onestream_to_df(
    base_url: str,
    application: str,
    api: Literal["data_adapter", "sql_query"],
    credentials_secret: str | None = None,
    config_key: str | None = None,
    params: dict[str, str] | None = None,
    if_empty: Literal["warn", "skip", "fail"] = "warn",
    **kwargs,
) -> pd.DataFrame:
    """Extract data from a OneStream API endpoint into a Pandas DataFrame.

    This is a generic task that works with multiple OneStream endpoint types
    (data_adapter, sql_query) by leveraging the internal dispatcher pattern.
    All endpoint-specific parameters should be passed as keyword arguments.

    Args:
        base_url (str): The base URL of the OneStream API.
        application (str): The name of the OneStream application.
        api (Literal["data_adapter", "sql_query"]): The API endpoint type.
        credentials_secret (str, optional): Azure Key Vault secret containing
            OneStream credentials. If neither this nor config_key is provided,
            an error is raised. Defaults to None.
        config_key (str, optional): Alternate config key from the local
            environment file. Defaults to None.
        params (dict[str, str], optional): Additional query parameters.
            Defaults to None.
        if_empty (Literal["warn", "skip", "fail"], optional): Behavior when
            the query returns empty data. Defaults to "warn".
        **kwargs: Endpoint-specific parameters.
            For "data_adapter":
                - adapter_name (str): Name of the OneStream adapter to query.
                - workspace_name (str, optional): Name of the workspace.
                  Defaults to "MainWorkspace".
                - adapter_response_key (str, optional): Key in the JSON
                  response. Defaults to "Results".
                - custom_subst_vars (dict[str, list[Any]], optional): Custom
                  substitution variables.
            For "sql_query":
                - sql_query (str): The SQL query to execute.
                - custom_subst_vars (dict, optional): Custom substitution
                  variables.
                - db_location (str, optional): Database location.
                  Defaults to "Application".
                - results_table_name (str, optional): Results table name.
                  Defaults to "Results".
                - external_db (str, optional): External database name.
                  Defaults to "".

    Raises:
        MissingSourceCredentialsError: If neither credentials_secret nor
            config_key is provided.

    Returns:
        pd.DataFrame: The extracted data as a Pandas DataFrame.
    """
    if not (credentials_secret or config_key):
        raise MissingSourceCredentialsError

    credentials = get_credentials(credentials_secret)  # type: ignore
    onestream = OneStream(
        base_url=base_url,
        application=application,
        credentials=credentials,
        config_key=config_key,
        params=params,
    )

    return onestream.to_df(api=api, if_empty=if_empty, **kwargs)


@task(retries=3, log_prints=True, retry_delay_seconds=10, timeout_seconds=60 * 60)
def onestream_run_data_management_seq(
    base_url: str,
    application: str,
    dm_seq_name: str,
    credentials_secret: str | None = None,
    config_key: str | None = None,
    custom_subst_vars: dict[str, list[Any]] | None = None,
    params: dict[str, str] | None = None,
) -> requests.Response:
    """Run a OneStream Data Management Sequence.

    Args:
        base_url (str): OneStream base server URL.
        application (str): OneStream application name.
        dm_seq_name (str): Data Management Sequence name.
        credentials_secret (str, optional): Key Vault secret name. Defaults to None.
        config_key (str,optional): Viadot config key. Defaults to None.
        custom_subst_vars (dict[str, list[Any]], optional): A dictionary mapping
            substitution variable names to lists of possible values.Substition variables
            used as substitution variables that are mapped to the column names in the
            Data Adapter configuration.They provides a kind of SQL WHERE clause for rows
            filtering where variable refers to a name of a set containings a list
            of possible values.
            For example: "prm_activity_region":["MK", "LG"]
                        refers to column UD3 that has custom variable mapping of
                        prm_activity_region.Data will be extracted only for rows where
                        the values (MK,LG) are present.
        params (dict[str, str], optional): API parameters. Defaults to None.

    Returns:
        requests.Response: Sequence execution response.
    """
    if not (credentials_secret or config_key):
        raise MissingSourceCredentialsError

    credentials = get_credentials(credentials_secret)
    onestream = OneStream(
        base_url=base_url,
        application=application,
        credentials=credentials,
        config_key=config_key,
        params=params,
    )

    return onestream.execute(
        api="run_data_management_seq",
        dm_seq_name=dm_seq_name,
        custom_subst_vars=custom_subst_vars,
    )
