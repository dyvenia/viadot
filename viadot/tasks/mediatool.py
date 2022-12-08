import time
import json
from typing import Any, Dict, List, Literal
import pandas as pd

from datetime import datetime, timedelta
from prefect import Task
from prefect.utilities import logging
from prefect.utilities.tasks import defaults_from_attrs

from viadot.sources import Mediatool
from viadot.config import local_config
from viadot.tasks import AzureKeyVaultSecret
from viadot.exceptions import CredentialError
from ..task_utils import credentials_loader, concat_dfs, union_dfs_task

logger = logging.get_logger()


class MediatoolToDF(Task):
    def __init__(
        self,
        organization_id: List[str] = None,
        mediatool_credentials: str = None,
        mediatool_credentials_secret: str = None,
        file_extension: Literal["parquet", "csv"] = "csv",
        file_path: str = "",
        *args: List[Any],
        **kwargs: Dict[str, Any],
    ):
        """ """

        self.file_extension = file_extension
        self.file_path = file_path
        if mediatool_credentials is None:
            try:
                self.mediatool_credentials = credentials_loader(
                    credentials_secret=mediatool_credentials_secret
                )
            except:
                print("Credentials no provided! / or secret_name")
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
        get_columns_from_right_df: List[str] = None,
        how: str = "left",
        column_name_to_replace=None,
    ):
        if get_columns_from_right_df is None:
            get_columns_from_right_df = df_right.columns

        df_merged = df_left.merge(
            df_right[get_columns_from_right_df],
            left_on=left_on,
            right_on=right_on,
            how=how,
        )
        # columns = [column for column in df_merged.columns if column not in ('organizationId','_id_y')]
        return df_merged

    def __call__(self, *args, **kwargs):
        """Download Mediatool data to DF"""
        return super().__call__(*args, **kwargs)

    @defaults_from_attrs()
    def run(self, organization_ids: List[str] = None):
        """Return DF from source"""

        mediatool = Mediatool(credentials=self.mediatool_credentials)
        df_orgs = mediatool.get_organizations(self.mediatool_credentials["USER_ID"])

        for organization_id in organization_ids:
            print(f"for: {organization_id}")
            df_m_entries = mediatool.get_media_entries(organization_id=organization_id)
            df_veh = mediatool.get_vehicles(organization_id=organization_id)
            df_camp = mediatool.get_campaigns(organization_id=organization_id)

            media_entries = mediatool.get_media_entries(
                organization_id=organization_id, return_dataframe=False
            )
            media_type_ids = [
                media_entry["mediaTypeId"] for media_entry in media_entries
            ]
            unique_media_type_ids = set(media_type_ids)
            df_m_types = mediatool.get_media_types(unique_media_type_ids)

            df_merged_entries_orgs = self.join_dfs(
                df_left=df_m_entries,
                df_right=df_orgs,
                left_on="organizationId",
                right_on="id_org",
                get_columns_from_right_df=["id_org", "organization_name"],
                how="left",
            )

            df_merged_campaigns = self.join_dfs(
                df_left=df_merged_entries_orgs,
                df_right=df_camp,
                left_on="campaignId",
                right_on="_id",
                get_columns_from_right_df=["_id", "name", "conventionalName"],
                how="left",
            )

            df_merged_vehicles = self.join_dfs(
                df_left=df_merged_campaigns,
                df_right=df_veh,
                left_on="vehicleId",
                right_on="_id",
                get_columns_from_right_df=["_id", "name"],
                how="left",
            )

            df_merged_media_types = self.join_dfs(
                df_left=df_merged_vehicles,
                df_right=df_m_types,
                left_on="mediaTypeId",
                right_on="id",
                get_columns_from_right_df=["id", "media_type_name"],
                how="left",
            )

        # columns_to_exclude = [
        #     column for column in df_merged_media_types if "_id_" in column
        # ]

        return df_merged_media_types
