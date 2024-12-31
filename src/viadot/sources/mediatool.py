"""'mediatool.py'."""

import json

import pandas as pd
from pydantic import BaseModel

from viadot.config import get_source_credentials
from viadot.exceptions import APIError, CredentialError
from viadot.sources.base import Source
from viadot.utils import add_viadot_metadata_columns, handle_api_response


class MediatoolCredentials(BaseModel):
    """Checking for values in Mediatool credentials dictionary.

    Two key values are held in the Mediatool connector:
        - user_id: The unique ID for the organization.
        token: A unique token to be used as the password for API requests.

    Args:
        BaseModel (pydantic.main.ModelMetaclass): A base class for creating
            Pydantic models.
    """

    user_id: str
    token: str


class Mediatool(Source):
    """Class implementing the Mediatool API.

    Download data from Mediatool platform. Using Mediatool class user is able to
    download organizations, media entries, campaigns, vehicles, and media types data.
    """

    def __init__(
        self,
        *args,
        credentials: MediatoolCredentials | None = None,
        config_key: str | None = None,
        user_id: str | None = None,
        **kwargs,
    ):
        """Create an instance of the Mediatool class.

        Args:
            credentials (MediatoolCredentials, optional): Meditaool credentials.
                Defaults to None.
            config_key (str, optional): The key in the viadot config holding relevant
                credentials. Defaults to None.
            user_id (str, optional): User ID. Defaults to None.
        """
        credentials = credentials or get_source_credentials(config_key) or None
        if credentials is None:
            message = "Missing credentials."
            raise CredentialError(message)

        validated_creds = dict(MediatoolCredentials(**credentials))
        super().__init__(*args, credentials=validated_creds, **kwargs)

        self.header = {"Authorization": f"Bearer {credentials.get('token')}"}
        self.user_id = user_id or credentials.get("user_id")

    def _get_organizations(
        self,
        user_id: str | None = None,
    ) -> list[dict[str, str]]:
        """Get organizations data based on the user ID.

        Args:
            user_id (str, optional): User ID. Defaults to None.

        Returns:
            list[dict[str, str]]: A list of dicts will be returned.
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

        return list_organizations

    def _get_media_entries(
        self,
        organization_id: str,
    ) -> list[dict[str, str]]:
        """Data for media entries.

        Args:
            organization_id (str): Organization ID.

        Returns:
            list[dict[str, str]]: A list of dicts will be returned.
        """
        url = (
            "https://api.mediatool.com/searchmediaentries?q="
            + f'{{"organizationId": "{organization_id}"}}'
        )

        response = handle_api_response(
            url=url,
            headers=self.header,
            method="GET",
        )
        response_dict = json.loads(response.text)

        return response_dict["mediaEntries"]

    def _get_vehicles(
        self,
        vehicle_ids: list[str],
    ) -> list[dict[str, str]]:
        """Vehicles data based on the organization IDs.

        Args:
            vehicle_ids (list[str]): List of organization IDs.

        Raises:
            APIError: Mediatool API does not recognise the vehicle id.

        Returns:
            list[dict[str, str]]: A list of dicts will be returned.
        """
        response_list = []
        missing_vehicles = []

        for vid in vehicle_ids:
            url = f"https://api.mediatool.com/vehicles/{vid}"
            try:
                response = handle_api_response(
                    url=url,
                    headers=self.header,
                    method="GET",
                )
            except APIError:
                missing_vehicles.append(vid)
            else:
                response_dict = json.loads(response.text)
                response_list.append(response_dict["vehicle"])

        if missing_vehicles:
            self.logger.error(f"Vehicle were not found for: {missing_vehicles}.")

        return response_list

    def _get_campaigns(
        self,
        organization_id: str,
    ) -> list[dict[str, str]]:
        """Campaign data based on the organization ID.

        Args:
            organization_id (str): Organization ID.

        Returns:
            list[dict[str, str]]: A list of dicts will be returned.
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

        return response_dict["campaigns"]

    def _get_media_types(
        self,
        media_type_ids: list[str],
    ) -> list[dict[str, str]]:
        """Media types data based on the media types ID.

        Args:
            media_type_ids (list[str]): List of media type IDs.

        Returns:
            list[dict[str, str]]: A list of dicts will be returned.
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

        return list_media_types

    def _to_records(
        self,
        endpoint: Literal[
            "organizations", "media_entries", "vehicles", "campaigns", "media_types"
        ],
        organization_id: str | None = None,
        vehicle_ids: list[str] | None = None,
        media_type_ids: list[str] | None = None,
    ) -> list[dict[str, str]]:
        """Connects to the Mediatool API and retrieves data for the specified endpoint.

        Args:
            endpoint (Literal["organizations", "media_entries", "vehicles", "campaigns",
                "media_types"]): The API endpoint to fetch data from.
            organization_id (str, optional): Organization ID. Defaults to None.
            vehicle_ids (list[str]): List of organization IDs. Defaults to None.
            media_type_ids (list[str]): List of media type IDs. Defaults to None.

        Returns:
            list[dict[str, str]]: A list of records containing the retrieved data.
        """
        if endpoint == "organizations":
            return self._get_organizations(self.user_id)

        if endpoint == "media_entries":
            return self._get_media_entries(organization_id=organization_id)

        if endpoint == "vehicles":
            return self._get_vehicles(vehicle_ids=vehicle_ids)

        if endpoint == "campaigns":
            return self._get_campaigns(organization_id=organization_id)

        if endpoint == "media_types":
            return self._get_media_types(media_type_ids=media_type_ids)
        return None

    def fetch_and_transform(
        self,
        endpoint: Literal[
            "organizations", "media_entries", "vehicles", "campaigns", "media_types"
        ],
        if_empty: str = "warn",
        organization_id: str | None = None,
        vehicle_ids: list[str] | None = None,
        media_type_ids: list[str] | None = None,
        columns: list[str] | None = None,
    ) -> pd.DataFrame:
        """Pandas Data Frame with the data in the Response object and metadata.

        Args:
            if_empty (str, optional): What to do if a fetch produce no data.
                Defaults to "warn

        Returns:
            pd.Dataframe: The response data as a Pandas Data Frame plus viadot metadata.
        """
        records = self._to_records(
            endpoint, organization_id, vehicle_ids, media_type_ids
        )

        data_frame = pd.DataFrame.from_dict(records)  # type: ignore

        if endpoint == "campaigns":
            data_frame.replace(
                to_replace=[r"\\t|\\n|\\r", "\t|\n|\r"],
                value=["", ""],
                regex=True,
                inplace=True,
            )

        if endpoint:
            # Endpoint name is added to the end of the column name to make it unique.
            data_frame = data_frame.rename(
                columns={
                    column_name: f"{column_name}_{endpoint}"
                    for column_name in data_frame.columns
                }
            )

        if columns:
            if set(columns).issubset(set(data_frame.columns)):
                data_frame = data_frame[columns]
            elif not set(columns).issubset(set(data_frame.columns)):
                self.logger.error(
                    f"Columns '{', '.join(columns)}' are incorrect. "
                    + "Whole dictionary for 'mediaEntries' will be returned."
                )

        if data_frame.empty:
            self._handle_if_empty(
                if_empty=if_empty,
                message="The response does not contain any data.",
            )
        else:
            self.logger.info(
                "Successfully downloaded data from "
                + f"the Mediatool API ({endpoint})."
            )

        return data_frame
