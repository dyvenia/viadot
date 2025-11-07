"""Tasks for OneStream API."""

from typing import Any

import pandas as pd
from prefect import task
import requests

from viadot.orchestration.prefect.exceptions import MissingSourceCredentialsError
from viadot.orchestration.prefect.utils import get_credentials
from viadot.sources.onestream import OneStream


# TODO fix types in the docstring
@task(retries=3, log_prints=True, retry_delay_seconds=10, timeout_seconds=60 * 60)
def onestream_get_agg_adapter_endpoint_data_to_df(
    server_url: str,
    application: str,
    adapter_name: str,
    credentials_secret: str | None = None,
    config_key: str = "onestream",
    workspace_name: str = "MainWorkspace",
    adapter_response_key: str = "Results",
    custom_subst_vars: dict[str, list[Any]] | None = None,
    api_params: dict[str, str] | None = None,
) -> pd.DataFrame:
    """Retrieves and aggregates data from a OneStream Data Adapter.

    Processes custom variables to generate combinations and fetch data.

    Args:
        server_url (str): OneStream server URL.
        application (str): OneStream application name.
        adapter_name (str): Data Adapter name to query.
        credentials_secret (str, optional): Key Vault secret name.
            Defaults to None.
        config_key (str): Viadot config key.
            Defaults to "onestream".
        workspace_name (str): OneStream workspace name.
            Defaults to "MainWorkspace".
        adapter_response_key (str): Key in the JSON response that contains
            the adapter's returned data. Defaults to "Results".
        custom_subst_vars (dict[str, list[Any]], optional): A dictionary mapping
            substitution variable names to lists of possible values.
            Values can be of any type that can be converted to strings, as they are
            used as substitution variables in the Data Adapter.Defaults to None.
        api_params (dict[str, str], optional): API parameters.
            Defaults to None.

    Returns:
        pd.DataFrame: Variable combinations mapped to their data as Pandas Data Frame.
    """
    if not (credentials_secret or config_key):
        raise MissingSourceCredentialsError

    credentials = get_credentials(credentials_secret)  # type: ignore
    onestream = OneStream(
        server_url=server_url,
        application=application,
        credentials=credentials,
        config_key=config_key,
        api_params=api_params,
    )

    data = onestream.get_agg_adapter_endpoint_data(
        adapter_name=adapter_name,
        workspace_name=workspace_name,
        adapter_response_key=adapter_response_key,
        custom_subst_vars=custom_subst_vars,
    )
    return onestream._to_df(data=data)


@task(retries=3, log_prints=True, retry_delay_seconds=10, timeout_seconds=60 * 60)
def onestream_get_agg_sql_data_to_df(
    server_url: str,
    application: str,
    sql_query: str,
    credentials_secret: str | None = None,
    config_key: str = "onestream",
    custom_subst_vars: dict[str, list[Any]] | None = None,
    db_location: str = "Application",
    results_table_name: str = "Results",
    external_db: str = "",
    api_params: dict[str, str] | None = None,
) -> pd.DataFrame:
    """Retrieves and aggregates SQL data from OneStream.

    Args:
        server_url (str): OneStream server URL.
        application (str): OneStream application name.
        sql_query (str): SQL query to execute.
        credentials_secret (str, optional): Key Vault secret name.
            Defaults to None.
        config_key (str): Viadot config key.
            Defaults to "onestream".
        custom_subst_vars (dict[str, list[Any]], optional): A dictionary mapping
            substitution variable names to lists of possible values.
            Values can be of any type that can be converted to strings, as they
            are used as substitution variables in the Data Adapter.Defaults to None.
        db_location (str): Database location path.
            Defaults to "Application".
        results_table_name (str): Results table name.
            Defaults to "Results".
        external_db (str): External database name.
            Defaults to "".
        api_params (dict[str, str], optional): API parameters.
            Defaults to None.

    Returns:
        pd.DataFrame: Aggregated SQL data as Pandas Data Frame.
    """
    if not (credentials_secret or config_key):
        raise MissingSourceCredentialsError

    credentials = get_credentials(credentials_secret)
    onestream = OneStream(
        server_url=server_url,
        application=application,
        credentials=credentials,
        config_key=config_key,
        api_params=api_params,
    )

    data = onestream.get_agg_sql_data(
        sql_query=sql_query,
        custom_subst_vars=custom_subst_vars,
        db_location=db_location,
        results_table_name=results_table_name,
        external_db=external_db,
    )
    return onestream._to_df(data=data)


@task(retries=3, log_prints=True, retry_delay_seconds=10, timeout_seconds=60 * 60)
def onestream_run_data_management_seq(
    server_url: str,
    application: str,
    dm_seq_name: str,
    credentials_secret: str | None = None,
    config_key: str = "onestream",
    custom_subst_vars: dict[str, list[Any]] | None = None,
    api_params: dict[str, str] | None = None,
) -> requests.Response:
    """Runs a OneStream Data Management Sequence.

    Args:
        server_url (str): OneStream server URL.
        application (str): OneStream application name.
        dm_seq_name (str): Data Management Sequence name.
        credentials_secret (str, optional): Key Vault secret name.
            Defaults to None.
        config_key (str): Viadot config key.
            Defaults to "onestream".
        custom_subst_vars (dict[str, list[Any]], optional): A dictionary mapping
            substitution variable names to lists of possible values.
            Values can be of any type that can be converted to strings, as they
            are used as substitution variables in the Data Adapter.Defaults to None.
        db_location (str): Database location path.
        api_params (dict[str, str], optional): API parameters.
            Defaults to None.

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
        api_params=api_params,
    )

    return onestream.run_data_management_seq(
        dm_seq_name=dm_seq_name,
        custom_subst_vars=custom_subst_vars,
    )
