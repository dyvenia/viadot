"""
'mediatool.py'.

Prefect task wrapper for the Mediatool API connector.

This module provides an intermediate wrapper between the prefect flow and the connector:
- Generate the Mediatool API connector.
- Create and return a pandas Data Frame with the response of the API.

Typical usage example:
    data_frame = mediatool_to_df(
        credentials=credentials,
        config_key=config_key,
        azure_key_vault_secret=azure_key_vault_secret,
        organization_ids=organization_ids,
        media_entries_columns=media_entries_columns,
    )

Functions:
    mediatool_to_df(credentials, config_key, azure_key_vault_secret, organization_ids,
    media_entries_columns): Task to download data from Mediatool API.
"""  # noqa: D412

from typing import Any, Dict, List, Optional

import pandas as pd
from prefect import task

from viadot.orchestration.prefect.exceptions import MissingSourceCredentialsError
from viadot.orchestration.prefect.utils import get_credentials
from viadot.sources import Mediatool
from viadot.utils import join_dfs


@task(retries=3, log_prints=True, retry_delay_seconds=10, timeout_seconds=60 * 60)
def mediatool_to_df(
    credentials: Optional[Dict[str, Any]] = None,
    config_key: str = None,
    azure_key_vault_secret: Optional[str] = None,
    organization_ids: Optional[List[str]] = None,
    media_entries_columns: Optional[List[str]] = None,
) -> pd.DataFrame:
    """
    Task to download data from Mediatool API.

    Data from different endpoints are retrieved and combined. A final object is created
    containing data for all organizations from the list.

    Args:
        credentials (Optional[Dict[str, Any]], optional): Mediatool credentials as a
            dictionary. Defaults to None.
        config_key (str, optional): The key in the viadot config holding relevant
            credentials. Defaults to None.
        azure_key_vault_secret (Optional[str], optional): The name of the Azure Key
            Vault secret where credentials are stored. Defaults to None.
        organization_ids (Optional[List[str]], optional): List of organization IDs.
            Defaults to None.
        media_entries_columns (Optional[List[str]], optional): Columns to get from
                media entries. Defaults to None.

    Raises:
        ValueError: 'organization_ids' argument is None, and one is mandatory.
        ValueError: One 'organization_id' is not in organizations.

    Returns:
        pd.DataFrame: The response data as a Pandas Data Frame.
    """
    if not (azure_key_vault_secret or config_key or credentials):
        raise MissingSourceCredentialsError

    if not config_key:
        credentials = credentials or get_credentials(azure_key_vault_secret)

    mediatool = Mediatool(
        credentials=credentials,
        config_key=config_key,
    )
    # first method ORGANIZATIONS
    method = "organizations"
    organizations_data = mediatool.api_connection(get_data_from=method)
    df_organizations = mediatool.to_df(data=organizations_data, column_suffix=method)

    if organization_ids is None:
        raise ValueError("No organizations were defined.")

    list_of_organizations_df = []
    for organization_id in organization_ids:
        if organization_id in df_organizations["_id_organizations"].unique():
            print(f"Downloading data for: {organization_id} ...")

            # extract media entries per organization
            media_entries_data = mediatool.api_connection(
                get_data_from="media_entries",
                organization_id=organization_id,
            )
            df_media_entries = mediatool.to_df(
                data=media_entries_data, drop_columns=media_entries_columns
            )
            unique_vehicle_ids = df_media_entries["vehicleId"].unique()
            unique_media_type_ids = df_media_entries["mediaTypeId"].unique()

            # extract vehicles
            method = "vehicles"
            vehicles_data = mediatool.api_connection(
                get_data_from=method,
                vehicle_ids=unique_vehicle_ids,
            )
            df_vehicles = mediatool.to_df(data=vehicles_data, column_suffix=method)

            # extract campaigns
            method = "campaigns"
            campaigns_data = mediatool.api_connection(
                get_data_from=method,
                organization_id=organization_id,
            )
            df_campaigns = mediatool.to_df(data=campaigns_data, column_suffix=method)

            # extract media types
            method = "media_types"
            media_types_data = mediatool.api_connection(
                get_data_from=method,
                media_type_ids=unique_media_type_ids,
            )
            df_media_types = mediatool.to_df(
                data=media_types_data, column_suffix=method
            )

            # join media entries & organizations
            df_merged_entries_orgs = join_dfs(
                df_left=df_media_entries,
                df_right=df_organizations,
                left_on="organizationId",
                right_on="_id_organizations",
                columns_from_right_df=[
                    "_id_organizations",
                    "name_organizations",
                    "abbreviation_organizations",
                ],
                how="left",
            )

            # join the previous merge & campaigns
            df_merged_campaigns = join_dfs(
                df_left=df_merged_entries_orgs,
                df_right=df_campaigns,
                left_on="campaignId",
                right_on="_id_campaigns",
                columns_from_right_df=[
                    "_id_campaigns",
                    "name_campaigns",
                    "conventionalName_campaigns",
                ],
                how="left",
            )

            # join the previous merge & vehicles
            df_merged_vehicles = join_dfs(
                df_left=df_merged_campaigns,
                df_right=df_vehicles,
                left_on="vehicleId",
                right_on="_id_vehicles",
                columns_from_right_df=["_id_vehicles", "name_vehicles"],
                how="left",
            )

            # join the previous merge & media types
            df_merged_media_types = join_dfs(
                df_left=df_merged_vehicles,
                df_right=df_media_types,
                left_on="mediaTypeId",
                right_on="_id_media_types",
                columns_from_right_df=["_id_media_types", "name_media_types"],
                how="left",
            )

            list_of_organizations_df.append(df_merged_media_types)

        else:
            raise ValueError(
                f"Organization - {organization_id} not found in organizations list."
            )

    df_final = pd.concat(list_of_organizations_df)

    return df_final
