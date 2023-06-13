from typing import Any, Dict, List, Literal

import pandas as pd
from prefect import Task
from prefect.utilities import logging
from prefect.utilities.tasks import defaults_from_attrs

from viadot.task_utils import *

from ..sources import Mediatool

logger = logging.get_logger()


class MediatoolToDF(Task):
    """
    The MediatoolToDF task fetches data for media entries, combines them with other data from Mediatool endpoints,
    and returns data with names instead of IDs. User can provide list of organization IDs.
    """

    def __init__(
        self,
        organization_ids: List[str] = None,
        media_entries_columns: List[str] = None,
        mediatool_credentials: dict = None,
        mediatool_credentials_key: str = None,
        *args: List[Any],
        **kwargs: Dict[str, Any],
    ):
        """
        Task MediatoolToDF for joining all of the data for media_entries and return as dataframe.

        Args:
            organization_ids (List[str]): List of organization IDs.
            media_entries_columns (List[str], optional): List of media entries fields to download. Defaults to None.
            mediatool_credentials (dict, optional): Dictionary containing Mediatool credentials. Defaults to None.
            mediatool_credentials_key (str, optional): Key for Mediatool credentials. Defaults to None.
        """

        self.media_entries_columns = media_entries_columns
        self.organization_ids = organization_ids

        if mediatool_credentials is None:
            self.mediatool_credentials = credentials_loader.run(
                credentials_secret=mediatool_credentials_key,
            )

        else:
            self.mediatool_credentials = mediatool_credentials

        super().__init__(
            name="mediatool_to_df",
            *args,
            **kwargs,
        )

    def join_dfs(
        self,
        df_left: pd.DataFrame,
        df_right: pd.DataFrame,
        left_on: str,
        right_on: str,
        columns_from_right_df: List[str] = None,
        how: Literal["left", "right", "outer", "inner", "cross"] = "left",
    ) -> pd.DataFrame:
        """
        Combine the dataframes according to the chosen method.

        Args:
            df_left (pd.DataFrame): Left dataframe.
            df_right (pd.DataFrame): Right dataframe.
            left_on (str): Column or index level names to join on in the left DataFrame.
            right_on (str): Column or index level names to join on in the right DataFrame.
            columns_from_right_df (List[str], optional): List of column to get from right dataframe.
                Defaults to None.
            how (Literal["left", "right", "outer", "inner", "cross"], optional): Type of merge to be performed.
                Defaults to "left".

        Returns:
            pd.DataFrame: Final dataframe after merging.
        """
        if columns_from_right_df is None:
            columns_from_right_df = df_right.columns

        df_merged = df_left.merge(
            df_right[columns_from_right_df],
            left_on=left_on,
            right_on=right_on,
            how=how,
        )
        return df_merged

    def __call__(self, *args, **kwargs):
        """Download Mediatool data to DF."""
        return super().__call__(*args, **kwargs)

    @defaults_from_attrs("organization_ids", "media_entries_columns")
    def run(
        self, organization_ids: List[str] = None, media_entries_columns=None
    ) -> pd.DataFrame:
        """
        Run method. Data for different endpoints are retrieved and combined.
        Finally, a final data frame is created containing data for all organizations from the list.

        Args:
            organization_ids (List[str], optional): List of organization IDs. Defaults to None.
            media_entries_columns (List[str], optional): Columns to get from media entries. Defaults to None.

        Raises:
            ValueError: Value is raised if organization_id is not recognized.

        Returns:
            pd.DataFrame: Data frame containing all of the information for organization or list of organizations.
        """
        mediatool = Mediatool(credentials=self.mediatool_credentials)
        df_orgs = mediatool.get_organizations(self.mediatool_credentials["USER_ID"])

        list_of_dfs = []
        for organization_id in organization_ids:
            if organization_id in df_orgs["_id_organizations"].unique():
                logger.info(f"Downloading data for: {organization_id} ...")

                # extract data
                df_media_entries = mediatool.get_media_entries(
                    organization_id=organization_id,
                    columns=media_entries_columns,
                )
                df_camp = mediatool.get_campaigns(organization_id=organization_id)

                unique_vehicle_ids = df_media_entries["vehicleId"].unique()
                df_veh = mediatool.get_vehicles(vehicle_ids=unique_vehicle_ids)

                unique_media_type_ids = df_media_entries["mediaTypeId"].unique()
                df_m_types = mediatool.get_media_types(unique_media_type_ids)

                # join DFs
                df_merged_entries_orgs = self.join_dfs(
                    df_left=df_media_entries,
                    df_right=df_orgs,
                    left_on="organizationId",
                    right_on="_id_organizations",
                    columns_from_right_df=[
                        "_id_organizations",
                        "name_organizations",
                        "abbreviation_organizations",
                    ],
                    how="left",
                )

                df_merged_campaigns = self.join_dfs(
                    df_left=df_merged_entries_orgs,
                    df_right=df_camp,
                    left_on="campaignId",
                    right_on="_id_campaigns",
                    columns_from_right_df=[
                        "_id_campaigns",
                        "name_campaigns",
                        "conventionalName_campaigns",
                    ],
                    how="left",
                )

                df_merged_vehicles = self.join_dfs(
                    df_left=df_merged_campaigns,
                    df_right=df_veh,
                    left_on="vehicleId",
                    right_on="_id_vehicles",
                    columns_from_right_df=["_id_vehicles", "name_vehicles"],
                    how="left",
                )

                df_merged_media_types = self.join_dfs(
                    df_left=df_merged_vehicles,
                    df_right=df_m_types,
                    left_on="mediaTypeId",
                    right_on="_id_media_types",
                    columns_from_right_df=["_id_media_types", "name_media_types"],
                    how="left",
                )

                list_of_dfs.append(df_merged_media_types)

            else:
                raise ValueError(
                    f"Organization - {organization_id} not found in organizations list."
                )

            df_final = pd.concat(list_of_dfs)

        return df_final
