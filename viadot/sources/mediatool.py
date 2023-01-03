from datetime import date, timedelta
from typing import List
from ..exceptions import APIError
from .base import Source
from ..utils import handle_api_response
import json
import pandas as pd
import inspect


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

                self.logger.error("Credentials not found.")

        super().__init__(*args, credentials=credentials, **kwargs)

        self.organization_id = organization_id or self.credentials.get(
            "ORGANIZATION_ID"
        )
        self.user_id = user_id or self.credentials.get("USER_ID")

    def rename_columns(
        self, df: pd.DataFrame = None, source_name: str = None
    ) -> pd.DataFrame:
        """
        Function for renaming columns. Source name is added to the end of the column name to make it unique.

        Args:
            df (pd.DataFrame, optional): Data frame. Defaults to None.
            source_name (str, optional): Name of data source. Source name is adding to dataframe column names.
                Defaults to None.

        Returns:
            pd.DataFrame: Final dataframe after changes.
        """
        if isinstance(df, pd.DataFrame):
            source_name = source_name.split("get_")[-1]
            dict_mapped_names = {
                column: f"{column}_{source_name}" for column in df.columns
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
        for other endpoints.Returns DF or dict.

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
                self.logger.error(e)
            return df_filtered

        return response_dict["mediaEntries"]

    def get_campaigns(
        self, organization_id: str, return_dataframe: bool = True
    ) -> pd.DataFrame:
        """
        Get campaign data based on the organization ID. Returns DF or dict.

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
            function_name = inspect.stack()[0][3]
            df_updated = self.rename_columns(df=df, source_name=function_name)
            return df_updated

        return response_dict["campaigns"]

    def get_vehicles(
        self, organization_id: str, return_dataframe: bool = True
    ) -> pd.DataFrame:
        """
        Get vehicles data based on the organization ID. Returns DF or dict.

        Args:
            organization_id (str): Organization ID.
            return_dataframe (bool, optional): Return a dataframe if True. If set to False, return the data as a dict.
            Defaults to True.

        Returns:
            pd.DataFrame: Default return dataframe. If 'return_daframe=False' then return list of dicts.
        """
        url = f"https://api.mediatool.com/organizations/{organization_id}/vehicles"

        response = handle_api_response(
            url=url,
            headers=self.header,
            method="GET",
        )

        response_dict = json.loads(response.text)

        if return_dataframe is True:
            df = pd.DataFrame.from_dict(response_dict["vehicles"])
            function_name = inspect.stack()[0][3]
            df_updated = self.rename_columns(df=df, source_name=function_name)
            return df_updated

        return response_dict["vehicles"]

    def get_organizations(
        self, user_id: str = None, return_dataframe: bool = True
    ) -> pd.DataFrame:
        """
        Get organizations data based on the user ID. Returns DF or dict.

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
            try:
                list_organizations.append(
                    {
                        "_id": org["_id"],
                        "name": org["name"],
                        "abbreviation": org["abbreviation"],
                    }
                )
            except KeyError:
                list_organizations.append(
                    {
                        "_id": org["_id"],
                        "name": org["name"],
                        "abbreviation": None,
                    }
                )

        if return_dataframe is True:
            df = pd.DataFrame.from_dict(list_organizations)
            function_name = inspect.stack()[0][3]
            df_updated = self.rename_columns(df=df, source_name=function_name)
            return df_updated

        return list_organizations

    def get_media_types(
        self, media_type_ids: List[str], return_dataframe: bool = True
    ) -> pd.DataFrame:
        """
        Get media types data based on the media types ID. User have to provide list of media type IDs.
        Returns DF or dict.

        Args:
            media_type_ids (List[str]): List of media type IDs.
            return_dataframe (bool, optional): Return a dataframe if True. If set to False, return the data as a dict.
                Defaults to True.

        Returns:
            pd.DataFrame: Default return dataframe. If 'return_daframe=False' then return list of dicts.
        """
        list_media_types = []

        for id_media_type in media_type_ids:
            try:
                response = handle_api_response(
                    url=f"https://api.mediatool.com/mediatypes/{id_media_type}",
                    headers=self.header,
                    method="GET",
                )
                response_dict = json.loads(response.text)
                list_media_types.append(
                    {
                        "_id": response_dict["mediaType"]["_id"],
                        "name": response_dict["mediaType"]["name"],
                        "type": response_dict["mediaType"]["type"],
                    }
                )
            except (APIError, KeyError):
                list_media_types.append(
                    {
                        "_id": response_dict["mediaType"]["_id"],
                        "name": response_dict["mediaType"]["name"],
                        "type": None,  # response_dict["mediaType"]["type"],
                    }
                )

        if return_dataframe is True:
            df = pd.DataFrame.from_dict(list_media_types)
            function_name = inspect.stack()[0][3]
            df_updated = self.rename_columns(df=df, source_name=function_name)
            return df_updated

        return list_media_types
