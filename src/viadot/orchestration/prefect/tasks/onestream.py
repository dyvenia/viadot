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
def onestream_get_agg_adapter_endpoint_data_to_df(
    server_url: str,
    application: str,
    adapter_name: str,
    credentials_secret: str | None = None,
    config_key: str | None = None,
    workspace_name: str = "MainWorkspace",
    adapter_response_key: str = "Results",
    custom_subst_vars: dict[str, list[Any]] | None = None,
    params: dict[str, str] | None = None,
    if_empty: Literal["warn", "skip", "fail"] = "fail",
) -> pd.DataFrame:
    """Retrieve and aggregate data from a OneStream Data Adapter as a DataFrame.

    Processes custom variables to generate combinations and fetch data.

    Args:
        server_url (str): OneStream server URL.
        application (str): OneStream application name.
        adapter_name (str): Data Adapter name to query.
        credentials_secret (str, optional): Key Vault secret name. Defaults to None.
        config_key (str, optional): Viadot config key. Defaults to None.
        workspace_name (str): OneStream workspace name. Defaults to "MainWorkspace".
        adapter_response_key (str): Key in the JSON response that contains the adapter's
            returned data. Defaults to "Results".
        custom_subst_vars (dict[str, list[Any]], optional): Dictionary mapping
            substitution variable names to lists of possible values. Defaults to None.
        params (dict[str, str], optional): API parameters. Defaults to None.

    Returns:
        pd.DataFrame: Variable combinations mapped to their data as a Pandas DataFrame.
    """
    if not (credentials_secret or config_key):
        raise MissingSourceCredentialsError

    credentials = get_credentials(credentials_secret)  # type: ignore
    onestream = OneStream(
        server_url=server_url,
        application=application,
        credentials=credentials,
        config_key=config_key,
        params=params,
    )

    data = onestream.get_agg_adapter_endpoint_data(
        adapter_name=adapter_name,
        workspace_name=workspace_name,
        adapter_response_key=adapter_response_key,
        custom_subst_vars=custom_subst_vars,
    )
    return onestream.to_df(data=data, if_empty=if_empty)


@task(retries=3, log_prints=True, retry_delay_seconds=10, timeout_seconds=60 * 60)
def onestream_get_agg_sql_data_to_df(  # noqa: PLR0913
    server_url: str,
    application: str,
    sql_query: str,
    credentials_secret: str | None = None,
    config_key: str | None = None,
    custom_subst_vars: dict[str, list[Any]] | None = None,
    db_location: str = "Application",
    results_table_name: str = "Results",
    external_db: str = "",
    params: dict[str, str] | None = None,
    if_empty: Literal["warn", "skip", "fail"] = "fail",
) -> pd.DataFrame:
    """Retrieve and aggregate SQL data from OneStream as a DataFrame.

    Args:
        server_url (str): OneStream server URL.
        application (str): OneStream application name.
        sql_query (str): SQL query to execute.
        credentials_secret (str, optional): Key Vault secret name. Defaults to None.
        config_key (str,optional): Viadot config key. Defaults to None.
        custom_subst_vars (dict[str, list[Any]], optional): Dictionary mapping
            substitution variable names to lists of possible values. Defaults to None.
        db_location (str): Database location path. Defaults to "Application".
        results_table_name (str): Results table name. Defaults to "Results".
        external_db (str): External database name. Defaults to "".
        params (dict[str, str], optional): API parameters. Defaults to None.
        if_empty (Literal["warn", "skip", "fail"], optional): What to do if the SQL
            query returns no data. Defaults to "fail".

    Returns:
        pd.DataFrame: Aggregated SQL data as a Pandas DataFrame.
    """
    if not (credentials_secret or config_key):
        raise MissingSourceCredentialsError

    credentials = get_credentials(credentials_secret)
    onestream = OneStream(
        server_url=server_url,
        application=application,
        credentials=credentials,
        config_key=config_key,
        params=params,
    )

    data = onestream.get_agg_sql_data(
        sql_query=sql_query,
        custom_subst_vars=custom_subst_vars,
        db_location=db_location,
        results_table_name=results_table_name,
        external_db=external_db,
    )
    return onestream.to_df(data=data, if_empty=if_empty)


@task(retries=3, log_prints=True, retry_delay_seconds=10, timeout_seconds=60 * 60)
def onestream_run_data_management_seq(
    server_url: str,
    application: str,
    dm_seq_name: str,
    credentials_secret: str | None = None,
    config_key: str | None = None,
    custom_subst_vars: dict[str, list[Any]] | None = None,
    params: dict[str, str] | None = None,
) -> requests.Response:
    """Run a OneStream Data Management Sequence.

    Args:
        server_url (str): OneStream server URL.
        application (str): OneStream application name.
        dm_seq_name (str): Data Management Sequence name.
        credentials_secret (str, optional): Key Vault secret name. Defaults to None.
        config_key (str,optional): Viadot config key. Defaults to None.
        custom_subst_vars (dict[str, list[Any]], optional): Dictionary mapping
            substitution variable names to lists of possible values. Defaults to None.
        params (dict[str, str], optional): API parameters. Defaults to None.

    Returns:
        requests.Response: Sequence execution response.
    """
    if not (credentials_secret or config_key):
        raise MissingSourceCredentialsError

    credentials = get_credentials(credentials_secret)
    onestream = OneStream(
        server_url=server_url,
        application=application,
        credentials=credentials,
        config_key=config_key,
        params=params,
    )

    return onestream.run_data_management_seq(
        dm_seq_name=dm_seq_name,
        custom_subst_vars=custom_subst_vars,
    )
