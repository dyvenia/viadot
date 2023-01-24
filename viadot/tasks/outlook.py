from typing import Any, Dict, List
import pandas as pd
from prefect import Task
from prefect.utilities import logging
from prefect.utilities.tasks import defaults_from_attrs
from ..sources import Outlook

logger = logging.get_logger()


class OutlookToDF(Task):
    def __init__(
        self,
        mailbox_name: str = None,
        start_date: str = None,
        end_date: str = None,
        credentials: Dict[str, Any] = None,
        output_file_extension: str = ".csv",
        limit: int = 10000,
        timeout: int = 3600,
        *args: List[Any],
        **kwargs: Dict[str, Any],
    ):

        self.mailbox_name = mailbox_name
        self.start_date = start_date
        self.end_date = end_date
        self.output_file_extension = output_file_extension
        self.limit = limit
        self.credentials = credentials

        super().__init__(
            name="outlook_to_csv",
            timeout=timeout,
            *args,
            **kwargs,
        )

    def __call__(self, *args, **kwargs):
        """Download Outlook Messages to DF"""
        return super().__call__(*args, **kwargs)

    @defaults_from_attrs("mailbox_name", "start_date", "end_date", "limit")
    def run(
        self,
        mailbox_name: str,
        start_date: str = None,
        end_date: str = None,
        limit: int = 10000,
    ) -> pd.DataFrame:
        """
        Task for downloading data from the Outlook API to DF.

        Args:
            mailbox_name (str): Mailbox name.
            start_date (str, optional): A filtering start date parameter e.g. "2022-01-01". Defaults to None.
            end_date (str, optional): A filtering end date parameter e.g. "2022-01-02". Defaults to None.
            limit (str, optional): A limit to access last top messages. Defaults to 10_000.

        Returns:
            pd.DataFrame: The API GET as a pandas DataFrames from Outlook.
        """
        outlook = Outlook(
            credentials=self.credentials,
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
