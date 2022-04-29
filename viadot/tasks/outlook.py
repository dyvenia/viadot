import pandas as pd
from viadot.config import local_config
from typing import Any, Dict, List, Tuple
from prefect import Task
from prefect.utilities.tasks import defaults_from_attrs

from ..sources import Outlook


class OutlookToDF(Task):
    def __init__(
        self,
        mailbox_name: str = None,
        start_date: str = None,
        end_date: str = None,
        credentials: Dict[str, Any] = None,
        extension_file: str = ".csv",
        limit: int = 10000,
        *args: List[Any],
        **kwargs: Dict[str, Any]
    ):

        self.mailbox_name = mailbox_name
        self.start_date = start_date
        self.end_date = end_date
        self.extension_file = extension_file
        self.limit = limit

        try:
            DEFAULT_CREDENTIALS = local_config["OUTLOOK"]
        except KeyError:
            DEFAULT_CREDENTIALS = None

        self.credentials = credentials or DEFAULT_CREDENTIALS

        super().__init__(
            name="outlook_to_csv",
            *args,
            **kwargs,
        )

    def __call__(self, *args, **kwargs):
        """Download Outlook Mesagess to DF"""
        return super().__call__(*args, **kwargs)

    @defaults_from_attrs("mailbox_name", "start_date", "end_date", "limit")
    def run(
        self,
        mailbox_name: str,
        start_date: str = None,
        end_date: str = None,
        limit: int = 10000,
        nout=2,
    ) -> Tuple[int, int]:
        """
        Task for downloading data from the Outlook API to a CSV file.

        Args:
            mailbox_name (str): Mailbox name.
            start_date (str, optional): A filtering start date parameter e.g. "2022-01-01". Defaults to None.
            end_date (str, optional): A filtering end date parameter e.g. "2022-01-02". Defaults to None.

        Returns:
            pd.DataFrame: The API GET as a pandas DataFrame.
        """
        df_inbox, df_outbox = Outlook(
            mailbox_name=mailbox_name,
            start_date=start_date,
            end_date=end_date,
            limit=limit,
        ).to_df()
        return (df_inbox, df_outbox)
