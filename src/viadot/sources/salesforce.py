"""Salesforce API connector."""

from typing import Literal

import pandas as pd
from pydantic import BaseModel
from simple_salesforce import Salesforce as SimpleSalesforce
from simple_salesforce.exceptions import SalesforceMalformedRequest

from viadot.config import get_source_credentials
from viadot.exceptions import CredentialError
from viadot.sources.base import Source
from viadot.utils import add_viadot_metadata_columns


class SalesforceCredentials(BaseModel):
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


class Salesforce(Source):
    """Class implementing the Salesforce API.

    Documentation for this API is available at:
        https://developer.salesforce.com/docs/apis.
    """

    def __init__(
        self,
        *args,
        credentials: SalesforceCredentials | None = None,
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
                (sandbox). Can only be added if a username/password/security token
                is provided.
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

        validated_creds = dict(SalesforceCredentials(**credentials))
        super().__init__(*args, credentials=validated_creds, **kwargs)

        if env.upper() == "DEV" or env.upper() == "QA":
            self.salesforce = SimpleSalesforce(
                username=self.credentials["username"],
                password=self.credentials["password"],
                security_token=self.credentials["token"],
                domain=domain,
                client_id=client_id,
            )

        elif env.upper() == "PROD":
            self.salesforce = SimpleSalesforce(
                username=self.credentials["username"],
                password=self.credentials["password"],
                security_token=self.credentials["token"],
            )

        else:
            message = "The only available environments are DEV, QA, and PROD."
            raise ValueError(message)

    @add_viadot_metadata_columns
    def to_df(
        self,
        query: str | None = None,
        table: str | None = None,
        columns: list[str] | None = None,
        if_empty: Literal["warn", "skip", "fail"] = "warn",
    ) -> pd.DataFrame:
        """Downloads data from Salesforce API and returns the DataFrame.

        Args:
            if_empty (str, optional): What to do if a fetch produce no data.
                Defaults to "warn
            query (str, optional): The query to be used to download the data.
                Defaults to None.
            table (str, optional): Table name. Defaults to None.
            columns (list[str], optional): List of required columns. Requires `table`
                to be specified. Defaults to None.

        Returns:
            pd.DataFrame: Selected rows from Salesforce.
        """
        if not query:
            columns_str = ", ".join(columns) if columns else "FIELDS(STANDARD)"
            query = f"SELECT {columns_str} FROM {table}"  # noqa: S608

        data = self.salesforce.query(query).get("records")

        # Remove metadata from the data
        for record in data:
            record.pop("attributes")

        df = pd.DataFrame(data)

        if df.empty:
            self._handle_if_empty(
                if_empty=if_empty,
                message="The response does not contain any data.",
            )
        else:
            self.logger.info("Successfully downloaded data from the Salesforce API.")

        return df

    def upsert(
        self,
        df: pd.DataFrame,
        table: str,
        external_id: str | None = None,
        raise_on_error: bool = False,
    ) -> bool | None:
        """Upsert data into Salesforce.

        Args:
            df (pd.DataFrame): Selected rows from Salesforce.
            table (str): Table name.
            external_id (str, optional): External ID. Defaults to None.
            raise_on_error (bool, optional): Whether to raise on error.
                Defaults to False.

        Returns:
            bool: True if all records where processed successfully. None when df empty.

        Raises:
            ValueError: If the external_id is provided but not found in the DataFrame.
            ValueError: If upserting a record fails and raise_on_error is True.
        """
        if df.empty:
            self._handle_if_empty()
            return None

        if external_id and external_id not in df.columns:
            msg = f"Passed DataFrame does not contain column '{external_id}'."
            raise ValueError(msg)

        table_to_upsert = getattr(self.salesforce, table)
        records = df.to_dict("records")
        records_cp = records.copy()

        for record in records_cp:
            response = 0
            if external_id:
                if record[external_id] is None:
                    continue
                merge_key = f"{external_id}/{record[external_id]}"
                record.pop(external_id)
            else:
                merge_key = record.pop("Id")

            try:
                response = table_to_upsert.upsert(data=record, record_id=merge_key)
            except SalesforceMalformedRequest as e:
                msg = f"Upsert of record {merge_key} failed."
                if raise_on_error:
                    raise ValueError(msg) from e
                self.logger.warning(msg)

            codes = {200: "updated", 201: "created", 204: "updated"}

            if response not in codes:
                msg = f"Upsert failed for record: \n{record} with response {response}"
                if raise_on_error:
                    raise ValueError(msg)
                self.logger.warning(msg)
            else:
                self.logger.info(f"Successfully {codes[response]} record {merge_key}.")

        self.logger.info(
            f"Successfully upserted {len(records)} records into table '{table}'."
        )

        return True
