import copy
import json
import os
from datetime import timedelta
from typing import List, Any, Literal, Dict

import pandas as pd
import prefect
from prefect import Task
from prefect.tasks.secrets import PrefectSecret
from prefect.utilities import logging
from prefect.utilities.tasks import defaults_from_attrs

from ..exceptions import ValidationError
from ..sources import VeluxClub
from .azure_key_vault import AzureKeyVaultSecret

logger = logging.get_logger()


class VeluxClubToDF(Task):
    def __init__(
        self,
        source: Literal["jobs", "product", "company", "survey"],
        credentials: Dict[str, Any] = None,
        from_date: str = "2022-03-22",
        to_date: str = "",
        if_empty: str = "warn",
        retry_delay: timedelta = timedelta(seconds=10),
        timeout: int = 3600,
        report_name: str = "velux_club_to_df",
        *args: List[Any],
        **kwargs: Dict[str, Any],
    ):
        """
        Task to downloading data from Velux Club APIs to Pandas DataFrame.

        Args:
            source (str): The endpoint source to be accessed, has to be among these:
                ['jobs', 'product', 'company', 'survey'].
            credentials (Dict[str, Any], optional): Stores the credentials information. Defaults to None.
            from_date (str): Start date for the query, by default is the oldest date in the data, '2022-03-22'.
            to_date (str): End date for the query, if empty, datetime.today() will be used.
            if_empty (str, optional): What to do if query returns no data. Defaults to "warn".
            retry_delay (timedelta, optional): The delay between task retries. Defaults to 10 seconds.
            timeout (int, optional): The amount of time (in seconds) to wait while running this task before
                a timeout occurs. Defaults to 3600.
            report_name (str, optional): Stores the report name. Defaults to "velux_club_to_df".

        Returns: Pandas DataFrame
        """
        self.logger = prefect.context.get("logger")
        self.source = source
        self.credentials = credentials
        self.from_date = from_date
        self.to_date = to_date
        self.if_empty = if_empty
        self.retry_delay = retry_delay
        self.report_name = report_name

        super().__init__(
            name=self.report_name,
            timeout=timeout,
            *args,
            **kwargs,
        )

    def __call__(self, *args, **kwargs):
        """Download Velux Club data to Pandas DataFrame"""
        return super().__call__(*args, **kwargs)

    def run(self) -> pd.DataFrame:
        """
        Task run method.

        Returns:
            pd.DataFrame: The query result as a pandas DataFrame.
        """

        vc_obj = VeluxClub()

        vc_dataframe = vc_obj.get_response(
            source=self.source, from_date=self.from_date, to_date=self.to_date
        )

        return vc_dataframe
