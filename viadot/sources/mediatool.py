from datetime import date, timedelta
from typing import List
from ..exceptions import APIError
from .base import Source
from ..utils import handle_api_response
import json
import pandas as pd


class Mediatool(Source):
    """ """

    def __init__(
        self, credentials: dict = None, organization_id: str = None, *args, **kwargs
    ):
        """ """
        if credentials is not None:
            try:
                self.header = {"Authorization": f"Bearer {credentials.get('TOKEN')}"}
            except:
                print("Credentials not found.")
        self.organization_id = organization_id

        super().__init__(*args, credentials=credentials, **kwargs)

    def get_media_entries(
        self,
        organization_id: str,
        start_date: str = None,
        end_date: str = None,
        time_delta: int = 360,
        return_dataframe: bool = True,
    ):
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
            return pd.DataFrame.from_dict(response_dict["mediaEntries"])

        return response_dict["mediaEntries"]

    def get_campaigns(self, organization_id, return_dataframe: bool = True):
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
            return pd.DataFrame.from_dict(response_dict["campaigns"])

        return response_dict["campaigns"]

    def get_vehicles(self, organization_id, return_dataframe: bool = True):
        url = f"https://api.mediatool.com/organizations/{organization_id}/vehicles"

        response = handle_api_response(
            url=url,
            headers=self.header,
            method="GET",
        )
        response_dict = json.loads(response.text)

        if return_dataframe is True:
            return pd.DataFrame.from_dict(response_dict["vehicles"])

        return response_dict["vehicles"]

    def get_organizations(
        self, user_id: str, return_dataframe: bool = True
    ) -> List[dict]:
        """ """
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
                {"id_org": org["_id"], "organization_name": org["name"]}
            )

        if return_dataframe is True:
            return pd.DataFrame.from_dict(list_organizations)

        return list_organizations

    def get_media_types(
        self, media_type_ids: List, return_dataframe: bool = True
    ) -> List[dict]:
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
                        "id": response_dict["mediaType"]["_id"],
                        "media_type_name": response_dict["mediaType"]["name"],
                        "type": response_dict["mediaType"]["type"],
                    }
                )
            except (APIError, KeyError):
                list_media_types.append(
                    {
                        "id": response_dict["mediaType"]["_id"],
                        "media_type_name": response_dict["mediaType"]["name"],
                        "type": None,  # response_dict["mediaType"]["type"],
                    }
                )

        if return_dataframe is True:
            return pd.DataFrame.from_dict(list_media_types)

        return list_media_types
