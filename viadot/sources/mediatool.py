import inspect
import json
from datetime import date, timedelta
from typing import List

import pandas as pd
from prefect.utilities import logging

from ..exceptions import CredentialError
from ..utils import handle_api_response
from .base import Source

logger = logging.get_logger(__name__)


class Mediatool(Source):
    """
    Class for downloading data from Mediatool platform. Using Mediatool class user is able to download
    organizations, media entries, campaigns, vehicles, and media types data.
    """

    def __init__(
        self,
        credentials: dict,
        organization_id: str = None,
        user_id: str = None,
        *args,
        **kwargs,
    ):
        """
        Create an instance of the Mediatool class.

        Args:
            credentials (dict): Mediatool credentials. Credentials have to contain authorization 'TOKEN'.
            organization_id (str, optional): Organization ID. Defaults to None.
            user_id (str, optional): User ID. Defaults to None.
        """
        if credentials is not None:
            try:
                self.header = {"Authorization": f"Bearer {credentials.get('TOKEN')}"}
            except:
                raise CredentialError("Credentials not found.")

        super().__init__(*args, credentials=credentials, **kwargs)

        self.organization_id = organization_id or self.credentials.get(
            "ORGANIZATION_ID"
        )
        self.user_id = user_id or self.credentials.get("USER_ID")

    def rename_columns(
        self, df: pd.DataFrame = None, column_suffix: str = "rename"
    ) -> pd.DataFrame:
        """
        Function for renaming columns. Source name is added to the end of the column name to make it unique.

        Args:
            df (pd.DataFrame, optional): Data frame. Defaults to None.
            column_suffix (str, optional): String to be added at the end of column name. Defaults to "rename".

        Returns:
            pd.DataFrame: Final dataframe after changes.
        """
        if isinstance(df, pd.DataFrame):
            column_suffix = column_suffix.split("get_")[-1]
            dict_mapped_names = {
                column_name: f"{column_name}_{column_suffix}"
                for column_name in df.columns
            }
            df_updated = df.rename(columns=dict_mapped_names)
            return df_updated
        else:
            raise TypeError("df object given to the function is not a data frame.")

    def get_media_entries(
        self,
        organization_id: str,
        columns: str = None,
        start_date: str = None,
        end_date: str = None,
        time_delta: int = 360,
        return_dataframe: bool = True,
    ) -> pd.DataFrame:
        """
        Get data for media entries. This is a main function. Media entries contain IDs for most of the fields
        for other endpoints.Returns DataFrame or Dict.

        Args:
            organization_id (str): Organization ID.
            columns (str, optional): What columns should be extracted. Defaults to None.
            start_date (str, optional): Start date e.g '2022-01-01'. Defaults to None.
            end_date (str, optional): End date e.g '2022-01-01'. Defaults to None.
            time_delta (int, optional): The number of days to retrieve from 'today' (today - time_delta). Defaults to 360.
            return_dataframe (bool, optional): Return a dataframe if True. If set to False, return the data as a dict.
                Defaults to True.

        Returns:
            pd.DataFrame: Default return dataframe If 'return_daframe=False' then return list of dicts.
        """
        today = date.today()

        if start_date is None:
            start_date = str(today - timedelta(time_delta))
        if end_date is None:
            start_date = str(today)

        url = f'https://api.mediatool.com/searchmediaentries?q={{"organizationId": "{organization_id}"}}'

        response = handle_api_response(
            url=url,
            headers=self.header,
            method="GET",
        )
        response_dict = json.loads(response.text)

        if return_dataframe is True:
            df = pd.DataFrame.from_dict(response_dict["mediaEntries"])
            if columns is None:
                columns = df.columns
            try:
                df_filtered = df[columns]
            except KeyError as e:
                logger.info(e)
            return df_filtered

        return response_dict["mediaEntries"]

    def get_campaigns(
        self, organization_id: str, return_dataframe: bool = True
    ) -> pd.DataFrame:
        """
        Get campaign data based on the organization ID. Returns DataFrame or Dict.

        Args:
            organization_id (str): Organization ID.
            return_dataframe (bool, optional): Return a dataframe if True. If set to False, return the data as a dict.
                Defaults to True.

        Returns:
            pd.DataFrame: Default return dataframe If 'return_daframe=False' then return list of dicts.
        """
        url_campaigns = (
            f"https://api.mediatool.com/organizations/{organization_id}/campaigns"
        )

        response = handle_api_response(
            url=url_campaigns,
            headers=self.header,
            method="GET",
        )
        response_dict = json.loads(response.text)

        if return_dataframe is True:
            df = pd.DataFrame.from_dict(response_dict["campaigns"])
            df.replace(
                to_replace=[r"\\t|\\n|\\r", "\t|\n|\r"],
                value=["", ""],
                regex=True,
                inplace=True,
            )
            function_name = inspect.stack()[0][3]
            df_updated = self.rename_columns(df=df, column_suffix=function_name)
            return df_updated

        return response_dict["campaigns"]

    def get_vehicles(
        self,
        vehicle_ids: List[str],
        return_dataframe: bool = True,
    ) -> pd.DataFrame:
        """
        Get vehicles data based on the organization IDs. Returns DataFrame or Dict.

        Args:
            vehicle_ids (List[str]): List of organization IDs.
            return_dataframe (bool, optional): Return a dataframe if True. If set to False, return the data as a dict.
                Defaults to True.

        Returns:
            pd.DataFrame: Default return dataframe. If 'return_daframe=False' then return list of dicts.
        """
        response_dict = {}
        dfs = []
        missing_vehicles = []

        for id in vehicle_ids:
            url = f"https://api.mediatool.com/vehicles/{id}"
            try:
                response = handle_api_response(
                    url=url,
                    headers=self.header,
                    method="GET",
                )
            except Exception:
                missing_vehicles.append(id)

            else:
                response_dict = json.loads(response.text)
                df_single = pd.DataFrame(response_dict["vehicle"], index=[0])
                dfs.append(df_single)

        if missing_vehicles:
            logger.error(f"Vehicle were not found for: {missing_vehicles}.")

        if return_dataframe is True:
            if len(dfs) > 0:
                df = pd.concat(dfs)
                function_name = inspect.stack()[0][3]
                df_updated = self.rename_columns(df=df, column_suffix=function_name)
                return df_updated
            return None

        return response_dict["vehicles"]

    def get_organizations(
        self, user_id: str = None, return_dataframe: bool = True
    ) -> pd.DataFrame:
        """
        Get organizations data based on the user ID. Returns DataFrame or Dict.

        Args:
            user_id (str): User ID.
            return_dataframe (bool, optional): Return a dataframe if True. If set to False, return the data as a dict.
            Defaults to True.

        Returns:
            pd.DataFrame: Default return dataframe. If 'return_daframe=False' then return list of dicts.
        """
        user_id = user_id or self.user_id
        url_organizations = f"https://api.mediatool.com/users/{user_id}/organizations"

        response = handle_api_response(
            url=url_organizations,
            headers=self.header,
            method="GET",
        )
        response_dict = json.loads(response.text)
        organizations = response_dict["organizations"]

        list_organizations = []
        for org in organizations:
            list_organizations.append(
                {
                    "_id": org.get("_id"),
                    "name": org.get("name"),
                    "abbreviation": org.get("abbreviation"),
                }
            )

        if return_dataframe is True:
            df = pd.DataFrame.from_dict(list_organizations)
            function_name = inspect.stack()[0][3]
            df_updated = self.rename_columns(df=df, column_suffix=function_name)
            return df_updated

        return list_organizations

    def get_media_types(
        self, media_type_ids: List[str], return_dataframe: bool = True
    ) -> pd.DataFrame:
        """
        Get media types data based on the media types ID. User have to provide list of media type IDs.
        Returns DataFrame or Dict.

        Args:
            media_type_ids (List[str]): List of media type IDs.
            return_dataframe (bool, optional): Return a dataframe if True. If set to False, return the data as a dict.
                Defaults to True.

        Returns:
            pd.DataFrame: Default return dataframe. If 'return_daframe=False' then return list of dicts.
        """
        list_media_types = []
        for id_media_type in media_type_ids:
            response = handle_api_response(
                url=f"https://api.mediatool.com/mediatypes/{id_media_type}",
                headers=self.header,
                method="GET",
            )
            response_dict = json.loads(response.text)
            list_media_types.append(
                {
                    "_id": response_dict.get("mediaType").get("_id"),
                    "name": response_dict.get("mediaType").get("name"),
                    "type": response_dict.get("mediaType").get("type"),
                }
            )

        if return_dataframe is True:
            df = pd.DataFrame.from_dict(list_media_types)
            function_name = inspect.stack()[0][3]
            df_updated = self.rename_columns(df=df, column_suffix=function_name)
            return df_updated

        return list_media_types
