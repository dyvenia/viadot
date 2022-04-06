import os
from stat import SF_IMMUTABLE
from prefect.utilities import logging
from typing import Any, Dict, List
from ..config import local_config
from .base import Source
import pandas as pd
from simple_salesforce import Salesforce as SF
from simple_salesforce.exceptions import SalesforceMalformedRequest

logger = logging.get_logger(__name__)


class Salesforce(Source):
    """
    A class for pulling data from theSalesforce.
    Parameters
    ----------
    """

    def __init__(
        self,
        *args,
        credentials: Dict[str, Any] = None,
        domain: str,
        client_id: str = None,
        env: str = "QA",
        **kwargs,
    ):
        try:
            DEFAULT_CREDENTIALS = local_config["SALESFORCE"].get(env)
        except KeyError:
            DEFAULT_CREDENTIALS = None

        self.credentials = credentials or DEFAULT_CREDENTIALS or {}

        super().__init__(*args, credentials=self.credentials, **kwargs)

        if env == "QA":
            self.salesforce = SF(
                username=self.credentials["username"],
                password=self.credentials["password"],
                security_token=self.credentials["token"],
                domain=domain,
                client_id=client_id,
            )
        elif env == "DEV":
            self.salesforce = SF(
                username=self.credentials["username"],
                password=self.credentials["password"],
                security_token="",
                domain=domain,
                client_id=client_id,
            )
        else:
            raise ValueError("The only environments available are QA and DEV.")

    def upsert(
        self,
        dict: Dict[str, Any],
        table: str,
    ) -> None:
        if len(dict) == 0:
            raise ValueError(f"Dictionary is empty")

        table_to_upsert = getattr(self.salesforce, table)
        records = dict["records"]
        records_cp = records.copy()
        for record in records_cp:
            key = record["Id"]
            record.pop("Id")
            try:
                response = table_to_upsert.upsert(data=record, record_id=key)
            except SalesforceMalformedRequest as e:
                raise ValueError(f"Upsert of record {key} failed.") from e
            codes = {200: "updated", 201: "created", 204: "updated"}
            logger.info(f"Successfully {codes[response]} record {key}.")
            if response not in list(codes.keys()):
                raise ValueError(
                    f"Upsert failed for record: \n{record} with response {response}"
                )
        logger.info(
            f"Successfully upserted {len(records)} records into table '{table}'."
        )

    def download(self, table: str, columns: List[str] = None):
        query = ""
        separator = ","
        if columns:
            query = f"SELECT {separator.join(columns)} FROM {table}"
        else:
            query = f"SELECT FIELDS(STANDARD) FROM {table}"

        data_dict = self.salesforce.query(query)
        return data_dict

    def to_df(self, dict: Dict[str, Any]):
        if len(dict) > 0:
            df = pd.DataFrame(dict, columns=dict.keys())
        else:
            raise ValueError(f"Dictionary is empty.")
        return df
