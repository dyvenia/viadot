import json
import pandas as pd
from viadot.config import local_config
from typing import Any, Dict, List, Tuple
from prefect import Task
from prefect.utilities.tasks import defaults_from_attrs
from prefect.utilities import logging

from ..sources import Outlook
from .azure_key_vault import AzureKeyVaultSecret

logger = logging.get_logger()


class OutlookToDF(Task):
    def __init__(
        self,
        mailbox_name: str = None,
        start_date: str = None,
        end_date: str = None,
        credentials: Dict[str, Any] = None,
        credentials_secret: str = None,
        vault_name: str = None,
        output_file_extension: str = ".csv",
        limit: int = 10000,
        *args: List[Any],
        **kwargs: Dict[str, Any],
    ):
        """
        The task for fetch Outlook mail and saving data as the data frame.

        Args:
            mailbox_name (str): Mailbox name.
            start_date (str, optional): A filtering start date parameter e.g. "2022-01-01". Defaults to None.
            end_date (str, optional): A filtering end date parameter e.g. "2022-01-02". Defaults to None.
            credentials (Dict[str, Any], optional): Credentials (TENANT_ID, CLIENT_ID, CLIENT_SECRET) for the Outlook Azure Application
            provided as dictionary.
            credentials_secret (str, optional): The name of the Azure Key Vault secret containing a dictionary with
            ACCOUNT_NAME and Service Principal credentials (TENANT_ID, CLIENT_ID, CLIENT_SECRET) for the Outlook Azure Application.
            Defaults to None.
            vault_name (str, optional): The name of the vault from which to obtain the secrets. Defaults to None.
            output_file_extension (str, optional): Output file extension. Defaults to ".parquet".
            limit (int, optional): Number of fetched top messages. Defaults to 10000.
        """

        if credentials is None:
            if credentials_secret:
                credentials_str = AzureKeyVaultSecret(
                    credentials_secret, vault_name=vault_name
                ).run()
            else:
                self.credentials = None
        else:
            credentials_str = credentials

        if self.credentials is not None:
            self.credentials = json.loads(credentials_str)

        self.mailbox_name = mailbox_name
        self.start_date = start_date
        self.end_date = end_date
        self.output_file_extension = output_file_extension
        self.limit = limit

        super().__init__(
            name="outlook_to_df",
            *args,
            **kwargs,
        )

    def __call__(self, *args, **kwargs):
        """Download Outlook Messages to DF"""
        return super().__call__(*args, **kwargs)

    @defaults_from_attrs(
        "mailbox_name",
        "start_date",
        "end_date",
        "limit",
    )
    def run(
        self,
        mailbox_name: str,
        start_date: str = None,
        end_date: str = None,
        credentials: Dict[str, Any] = None,
        credentials_secret: str = None,
        vault_name: str = None,
        limit: int = 10000,
    ) -> pd.DataFrame:
        """
        Task for downloading data from the Outlook API to DF.

        Args:
            mailbox_name (str): Mailbox name.
            start_date (str, optional): A filtering start date parameter e.g. "2022-01-01". Defaults to None.
            end_date (str, optional): A filtering end date parameter e.g. "2022-01-02". Defaults to None.
            credentials (Dict[str, Any], optional): credentials (TENANT_ID, CLIENT_ID, CLIENT_SECRET) for the Azure Application
            provided as dictionary.
            credentials_secret (str, optional): The name of the Azure Key Vault secret containing a dictionary with
            ACCOUNT_NAME and Service Principal credentials (TENANT_ID, CLIENT_ID, CLIENT_SECRET) for the Azure Application.
            Defaults to None.
            vault_name (str, optional): The name of the vault from which to obtain the secrets. Defaults to None.
            limit (str, optional): A limit to access last top messages. Defaults to 10_000.

        Returns:
            pd.DataFrame: The API GET as a pandas DataFrames from Outlook.
        """
        credentials_str = None
        if credentials is None:
            if credentials_secret:
                credentials_str = AzureKeyVaultSecret(
                    credentials_secret, vault_name=vault_name
                ).run()
            else:
                credentials = self.credentials

        if credentials_str is not None:
            credentials = json.loads(credentials_str)

        outlook = Outlook(
            credentials=credentials,
            mailbox_name=mailbox_name,
            start_date=start_date,
            end_date=end_date,
            limit=limit,
        )
        df = outlook.to_df()

        logger.info(
            f"Downloaded the data from the '{outlook.mailbox_name}' into the Data Frame."
        )
        return df
