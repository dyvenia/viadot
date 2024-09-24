"""Salesforce API connector."""

from typing import Literal

import pandas as pd
from pydantic import BaseModel
from simple_salesforce import Salesforce

from viadot.config import get_source_credentials
from viadot.exceptions import CredentialError
from viadot.sources.base import Source
from viadot.utils import add_viadot_metadata_columns


class SalesForceCredentials(BaseModel):
    """Checking for values in Salesforce credentials dictionary.

    Two key values are held in the Salesforce connector:
        - username: The unique name for the organization.
        - password: The unique passwrod for the organization.
        - token: A unique token to be used as the password for API requests.

    Args:
        BaseModel (pydantic.main.ModelMetaclass): A base class for creating
            Pydantic models.
    """

    username: str
    password: str
    token: str


class SalesForce(Source):
    """Class implementing the Salesforce API.

    Documentation for this API is available at:
        https://developer.salesforce.com/docs/apis.
    """

    def __init__(
        self,
        *args,
        credentials: SalesForceCredentials | None = None,
        config_key: str = "salesforce",
        env: Literal["DEV", "QA", "PROD"] = "DEV",
        domain: str = "test",
        client_id: str = "viadot",
        **kwargs,
    ):
        """A class for downloading data from Salesforce.

        Args:
            credentials (dict(str, any), optional): Salesforce credentials as a
                dictionary. Defaults to None.
            config_key (str, optional): The key in the viadot config holding relevant
                credentials. Defaults to "salesforce".
            env (Literal["DEV", "QA", "PROD"], optional): Environment information,
                provides information about credential and connection configuration.
                Defaults to 'DEV'.
            domain (str, optional): Domain of a connection. Defaults to 'test'
                (sandbox). Can only be add if a username/password/security token
                is provide.
            client_id (str, optional): Client id, keep track of API calls.
                Defaults to 'viadot'.
        """
        credentials = credentials or get_source_credentials(config_key)

        if not (
            credentials.get("username")
            and credentials.get("password")
            and credentials.get("token")
        ):
            message = "'username', 'password' and 'token' credentials are required."
            raise CredentialError(message)

        validated_creds = dict(SalesForceCredentials(**credentials))
        super().__init__(*args, credentials=validated_creds, **kwargs)

        if env.upper() == "DEV" or env.upper() == "QA":
            self.salesforce = Salesforce(
                username=self.credentials["username"],
                password=self.credentials["password"],
                security_token=self.credentials["token"],
                domain=domain,
                client_id=client_id,
            )

        elif env.upper() == "PROD":
            self.salesforce = Salesforce(
                username=self.credentials["username"],
                password=self.credentials["password"],
                security_token=self.credentials["token"],
            )

        else:
            message = "The only available environments are DEV, QA, and PROD."
            raise ValueError(message)

        self.data = None

    def api_connection(
        self,
        query: str | None = None,
        table: str | None = None,
        columns: list[str] | None = None,
    ) -> None:
        """General method to connect to Salesforce API and generate the response.

        Args:
            query (str, optional): The query to be used to download the data.
                Defaults to None.
            table (str, optional): Table name. Defaults to None.
            columns (list[str], optional): List of required columns. Requires `table`
                to be specified. Defaults to None.
        """
        if not query:
            columns_str = ", ".join(columns) if columns else "FIELDS(STANDARD)"
            query = f"SELECT {columns_str} FROM {table}"

        self.data = self.salesforce.query(query).get("records")

        # Remove metadata from the data
        for record in self.data:
            record.pop("attributes")

    @add_viadot_metadata_columns
    def to_df(
        self,
        if_empty: str = "fail",
    ) -> pd.DataFrame:
        """Downloads the indicated data and returns the DataFrame.

        Args:
            if_empty (str, optional): What to do if a fetch produce no data.
                Defaults to "warn

        Returns:
            pd.DataFrame: Selected rows from Salesforce.
        """
        df = pd.DataFrame(self.data)

        if df.empty:
            self._handle_if_empty(
                if_empty=if_empty,
                message="The response does not contain any data.",
            )
        else:
            self.logger.info("Successfully downloaded data from the Mindful API.")

        return df
