from typing import Any, Dict, List, Literal, OrderedDict

import pandas as pd
from prefect.utilities import logging
from simple_salesforce import Salesforce as SF
from simple_salesforce.exceptions import SalesforceMalformedRequest

from ..config import local_config
from ..exceptions import CredentialError
from .base import Source

logger = logging.get_logger(__name__)


class Salesforce(Source):
    """
    A class for pulling data from theSalesforce.
    Args:
        domain (str): domain of a connection; defaults to 'test' (sandbox). Can be added only if built-in username/password/security token is provided.
        client_id (str): client id to keep the track of API calls.
        credentials (dict): credentials to connect with. If not provided, will read from local config file.
        env (Literal): environment information, provides information about credential and connection configuration; defaults to 'DEV'.
    ----------
    """

    def __init__(
        self,
        *args,
        domain: str = "test",
        client_id: str = "viadot",
        credentials: Dict[str, Any] = None,
        env: Literal["DEV", "QA", "PROD"] = "DEV",
        **kwargs,
    ):
        try:
            DEFAULT_CREDENTIALS = local_config["SALESFORCE"].get(env)
        except KeyError:
            DEFAULT_CREDENTIALS = None

        self.credentials = credentials or DEFAULT_CREDENTIALS or {}

        if self.credentials is None:
            raise CredentialError("Credentials not found.")

        super().__init__(*args, credentials=self.credentials, **kwargs)

        if env.upper() == "DEV":
            self.salesforce = SF(
                username=self.credentials["username"],
                password=self.credentials["password"],
                security_token="",
                domain=domain,
                client_id=client_id,
            )
        elif env.upper() == "QA":
            self.salesforce = SF(
                username=self.credentials["username"],
                password=self.credentials["password"],
                security_token=self.credentials["token"],
                domain=domain,
                client_id=client_id,
            )
        elif env.upper() == "PROD":
            self.salesforce = SF(
                username=self.credentials["username"],
                password=self.credentials["password"],
                security_token=self.credentials["token"],
                domain=domain,
                client_id=client_id,
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

        if df.empty:
            logger.info("No data to upsert.")
            return

        if external_id and external_id not in df.columns:
            raise ValueError(
                f"Passed DataFrame does not contain column '{external_id}'."
            )

        table_to_upsert = getattr(self.salesforce, table)
        records = df.to_dict("records")
        records_cp = records.copy()

        for record in records_cp:
            response = 0
            if external_id:
                if record[external_id] is None:
                    continue
                else:
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
                else:
                    self.logger.warning(msg)

            codes = {200: "updated", 201: "created", 204: "updated"}

            if response not in codes:
                msg = f"Upsert failed for record: \n{record} with response {response}"
                if raise_on_error:
                    raise ValueError(msg)
                else:
                    self.logger.warning(msg)
            else:
                logger.info(f"Successfully {codes[response]} record {merge_key}.")

        logger.info(
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

        if df.empty:
            logger.info("No data to upsert.")
            return

        if external_id and external_id not in df.columns:
            raise ValueError(
                f"Passed DataFrame does not contain column '{external_id}'."
            )
        records = df.to_dict("records")
        response = 0
        try:
            response = self.salesforce.bulk.__getattr__(table).upsert(
                data=records, external_id_field=external_id, batch_size=batch_size
            )
        except SalesforceMalformedRequest as e:
            # Bulk insert didn't work at all.
            raise ValueError(f"Upsert of records failed: {e}") from e

        logger.info(f"Successfully upserted bulk records.")

        if any(result.get("success") is not True for result in response):
            # Upsert of some individual records failed.
            failed_records = [
                result for result in response if result.get("success") is not True
            ]
            msg = f"Upsert failed for records {failed_records} with response {response}"
            if raise_on_error:
                raise ValueError(msg)
            else:
                self.logger.warning(msg)

        logger.info(
            f"Successfully upserted {len(records)} records into table '{table}'."
        )

    def download(
        self, query: str = None, table: str = None, columns: List[str] = None
    ) -> List[OrderedDict]:
        if not query:
            if columns:
                columns_str = ", ".join(columns)
            else:
                columns_str = "FIELDS(STANDARD)"
            query = f"SELECT {columns_str} FROM {table}"
        records = self.salesforce.query(query).get("records")
        # Take trash out.
        _ = [record.pop("attributes") for record in records]
        return records

    def to_df(
        self,
        query: str = None,
        table: str = None,
        columns: List[str] = None,
        if_empty: str = None,
    ) -> pd.DataFrame:
        # TODO: handle if_empty, add typing (should be Literal)
        records = self.download(query=query, table=table, columns=columns)

        if not records:
            raise ValueError(f"Query produced no data.")

        return pd.DataFrame(records)
