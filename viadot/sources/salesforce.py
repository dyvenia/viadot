from typing import Any, Dict, List, Literal, OrderedDict

import pandas as pd
from simple_salesforce import Salesforce as SF
from simple_salesforce.exceptions import SalesforceMalformedRequest
from viadot.config import get_source_credentials
from viadot.exceptions import CredentialError
from viadot.sources.base import Source


class Salesforce(Source):
    """
    A class for downloading and upserting data from Salesforce.

    Args:
        domain (str, optional): Domain of a connection. Defaults to 'test' (sandbox).
            Can be added only if built-in username/password/security token is provided.
        client_id (str, optional): Client id to keep the track of API calls.
            Defaults to 'viadot'.
        env (Literal["DEV", "QA", "PROD"], optional): Environment information, provides information
            about credential and connection configuration. Defaults to 'DEV'.
        credentials (Dict[str, Any], optional): Credentials to connect with Salesforce.
            If not provided, will read from local config file. Defaults to None.
        config_key (str, optional): The key in the viadot config holding relevant credentials.
            Defaults to None.
    """

    def __init__(
        self,
        *args,
        domain: str = "test",
        client_id: str = "viadot",
        env: Literal["DEV", "QA", "PROD"] = "DEV",
        credentials: Dict[str, Any] = None,
        config_key: str = None,
        **kwargs,
    ):

        credentials = credentials or get_source_credentials(config_key) or {}

        if credentials is None:
            raise CredentialError("Please specify the credentials.")

        super().__init__(*args, credentials=credentials, **kwargs)

        if env.upper() == "DEV" or env.upper() == "QA":
            self.salesforce = SF(
                username=self.credentials.get("username"),
                password=self.credentials.get("password"),
                security_token=self.credentials.get("token"),
                domain=domain,
                client_id=client_id,
            )

        elif env.upper() == "PROD":
            self.salesforce = SF(
                username=self.credentials.get("username"),
                password=self.credentials.get("password"),
                security_token=self.credentials.get("token"),
            )

        else:
            raise ValueError("The only available environments are DEV, QA, and PROD.")

    def upsert(
        self,
        df: pd.DataFrame,
        table: str,
        external_id: str = None,
        raise_on_error: bool = False,
    ) -> None:
        """
        Performs upsert operations on the selected row in the table.

        Args:
            df (pd.DataFrame): The DataFrame to upsert. Only a single row can be upserted with this function.
            table (str): The table where the data should be upserted.
            external_id (str, optional): The external ID to use for the upsert. Defaults to None.
            raise_on_error (bool, optional): Whether to raise an exception if a row upsert fails.
                If False, we only display a warning. Defaults to False.
        """
        if df.empty:
            self.logger.info("No data to upsert.")
            return

        if external_id and external_id not in df.columns:
            raise ValueError(
                f"Passed DataFrame does not contain column '{external_id}'."
            )

        table_to_upsert = getattr(self.salesforce, table)
        records = df.to_dict("records")
        records_cp = records.copy()

        for record in records_cp:

            if external_id:
                # If the specified external ID is on the upsert line and has a value, it will be used as merge_key
                if record[external_id] is not None:
                    merge_key = f"{external_id}/{record[external_id]}"
                    record.pop(external_id)
            else:
                merge_key = record.pop("Id")

            try:
                response_code = table_to_upsert.upsert(data=record, record_id=merge_key)
            except SalesforceMalformedRequest as e:
                msg = f"Upsert of record {merge_key} failed."
                if raise_on_error:
                    raise ValueError(msg) from e
                else:
                    self.logger.warning(msg)

            valid_response_codes = {200: "updated", 201: "created", 204: "updated"}

            if response_code not in valid_response_codes:
                msg = f"Upsert failed for record: \n{record} with response code {response_code }"
                if raise_on_error:
                    raise ValueError(msg)
                else:
                    self.logger.warning(msg)
            else:
                self.logger.info(
                    f"Successfully {valid_response_codes[response_code]} record {merge_key}."
                )

        self.logger.info(
            f"Successfully upserted {len(records)} records into table '{table}'."
        )

    def bulk_upsert(
        self,
        df: pd.DataFrame,
        table: str,
        external_id: str = None,
        batch_size: int = 10000,
        raise_on_error: bool = False,
    ) -> None:
        """
        Performs upsert operations on multiple rows in a table.

        Args:
            df (pd.DataFrame): The DataFrame to upsert.
            table (str): The table where the data should be upserted.
            external_id (str, optional): The external ID to use for the upsert. Defaults to None.
            batch_size (int, optional): Number of records to be included in each batch of records
                that are sent to the Salesforce API for processing. Defaults to 10000.
            raise_on_error (bool, optional): Whether to raise an exception if a row upsert fails.
                If False, we only display a warning. Defaults to False.
        """
        if df.empty:
            self.logger.info("No data to upsert.")
            return

        if external_id and external_id not in df.columns:
            raise ValueError(
                f"Passed DataFrame does not contain column '{external_id}'."
            )
        records = df.to_dict("records")

        try:
            response_code = self.salesforce.bulk.__getattr__(table).upsert(
                data=records, external_id_field=external_id, batch_size=batch_size
            )
        except SalesforceMalformedRequest as e:
            # Bulk insert didn't work at all.
            raise ValueError(f"Upsert of records failed: {e}") from e

        self.logger.info(f"Successfully upserted bulk records.")

        if any(result.get("success") is not True for result in response_code):
            # Upsert of some individual records failed.
            failed_records = [
                result for result in response_code if result.get("success") is not True
            ]
            msg = f"Upsert failed for records {failed_records} with response code {response_code}"
            if raise_on_error:
                raise ValueError(msg)
            else:
                self.logger.warning(msg)

        self.logger.info(
            f"Successfully upserted {len(records)} records into table '{table}'."
        )

    def download(
        self, query: str = None, table: str = None, columns: List[str] = None
    ) -> List[OrderedDict]:
        """
        Dowload all data from the indicated table or the result of the specified query.

        Args:
            query (str, optional): Query for download the specific data. Defaults to None.
            table (str, optional): Table name. Defaults to None.
            columns (List[str], optional): List of columns which are needed,
                requires table argument. Defaults to None.

        Returns:
            List[OrderedDict]: Selected rows from Salesforce.
        """
        if not query:
            if columns:
                columns_str = ", ".join(columns)
            else:
                columns_str = "FIELDS(STANDARD)"
            query = f"SELECT {columns_str} FROM {table}"
        records = self.salesforce.query(query).get("records")
        # Remove metadata from the data
        _ = [record.pop("attributes") for record in records]
        return records

    def to_df(
        self,
        query: str = None,
        table: str = None,
        columns: List[str] = None,
    ) -> pd.DataFrame:
        """
        Converts the List returned by the download functions to a DataFrame.

        Args:
            query (str, optional): Query for download the specific data. Defaults to None.
            table (str, optional): Table name. Defaults to None.
            columns (List[str], optional): List of columns which are needed,
                requires table argument. Defaults to None.

        Returns:
            pd.DataFrame: Selected rows from Salesforce.
        """
        records = self.download(query=query, table=table, columns=columns)

        return pd.DataFrame(records)
