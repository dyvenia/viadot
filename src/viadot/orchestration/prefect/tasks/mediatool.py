"""'mediatool.py'."""

import pandas as pd
from prefect import get_run_logger, task

from viadot.orchestration.prefect.exceptions import MissingSourceCredentialsError
from viadot.orchestration.prefect.utils import get_credentials
from viadot.sources import Mediatool
from viadot.utils import join_dfs


@task(retries=3, log_prints=True, retry_delay_seconds=10, timeout_seconds=60 * 60)
def mediatool_to_df(
    config_key: str | None = None,
    azure_key_vault_secret: str | None = None,
    organization_ids: list[str] | None = None,
    media_entries_columns: list[str] | None = None,
) -> pd.DataFrame:
    """Task to download data from Mediatool API.

    Data from different endpoints are retrieved and combined. A final object is created
    containing data for all organizations from the list.

    Args:
        config_key (str, optional): The key in the viadot config holding relevant
            credentials. Defaults to None.
        azure_key_vault_secret (str, optional): The name of the Azure Key Vault secret
            where credentials are stored. Defaults to None.
        organization_ids (list[str], optional): List of organization IDs.
            Defaults to None.
        media_entries_columns (list[str], optional): Columns to get from media entries.
            Defaults to None.

    Raises:
        ValueError: 'organization_ids' argument is None, and one is mandatory.
        ValueError: One 'organization_id' is not in organizations.

    Returns:
        pd.DataFrame: The response data as a Pandas Data Frame.
    """
    logger = get_run_logger()

    if not (azure_key_vault_secret or config_key):
        raise MissingSourceCredentialsError

    if not config_key:
        credentials = get_credentials(azure_key_vault_secret)

    mediatool = Mediatool(
        credentials=credentials,
        config_key=config_key,
    )
    # first method ORGANIZATIONS
    method = "organizations"
    organizations_data = mediatool.api_connection(get_data_from=method)
    df_organizations = mediatool.to_df(data=organizations_data, column_suffix=method)

    if organization_ids is None:
        message = "No organizations were defined."
        raise ValueError(message)

    list_of_organizations_df = []
    for organization_id in organization_ids:
        if organization_id in df_organizations["_id_organizations"].unique():
            logger.info(f"Downloading data for: {organization_id} ...")

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
            message = (
                f"Organization - {organization_id} not found in organizations list."
            )
            raise ValueError(message)

    return pd.concat(list_of_organizations_df)
