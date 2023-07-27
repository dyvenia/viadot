import copy
import json
import os
from datetime import timedelta
from typing import Any, Dict, List, Literal

import pandas as pd
from prefect import Task
from prefect.tasks.secrets import PrefectSecret
from prefect.utilities import logging
from prefect.utilities.tasks import defaults_from_attrs

from viadot.task_utils import *

from ..sources import VidClub

logger = logging.get_logger()


class VidClubToDF(Task):
    def __init__(
        self,
        source: Literal["jobs", "product", "company", "survey"] = None,
        credentials: Dict[str, Any] = None,
        credentials_secret: str = "VIDCLUB",
        vault_name: str = None,
        from_date: str = "2022-03-22",
        to_date: str = None,
        timeout: int = 3600,
        report_name: str = "vid_club_to_df",
        *args: List[Any],
        **kwargs: Dict[str, Any],
    ):
        """
        Task to downloading data from Vid Club APIs to Pandas DataFrame.

        Args:
            source (Literal["jobs", "product", "company", "survey"], optional): The endpoint source to be accessed. Defaults to None.
            credentials (Dict[str, Any], optional): Stores the credentials information. Defaults to None.
            credentials_secret (str, optional): The name of the secret in Azure Key Vault or Prefect or local_config file. Defaults to "VIDCLUB".
            vault_name (str, optional): For credentials stored in Azure Key Vault. The name of the vault from which to obtain the secret. Defaults to None.
            from_date (str): Start date for the query, by default is the oldest date in the data, '2022-03-22'.
            to_date (str, optional): End date for the query. By default None, which will be executed as datetime.today().strftime("%Y-%m-%d") in code.
            timeout (int, optional): The amount of time (in seconds) to wait while running this task before
                a timeout occurs. Defaults to 3600.
            report_name (str, optional): Stores the report name. Defaults to "vid_club_to_df".

        Returns: Pandas DataFrame
        """
        self.source = source
        self.from_date = from_date
        self.to_date = to_date
        self.report_name = report_name
        self.credentials_secret = credentials_secret
        self.vault_name = vault_name

        if credentials is None:
            self.credentials = credentials_loader.run(
                credentials_secret=credentials_secret, vault_name=vault_name
            )
        else:
            self.credentials = credentials

        super().__init__(
            name=self.report_name,
            timeout=timeout,
            *args,
            **kwargs,
        )

    def __call__(self, *args, **kwargs):
        """Download Vid Club data to Pandas DataFrame"""
        return super().__call__(*args, **kwargs)

    @defaults_from_attrs(
        "source",
        "credentials",
        "from_date",
        "to_date",
    )
    def run(
        self,
        source: Literal["jobs", "product", "company", "survey"] = None,
        credentials: Dict[str, Any] = None,
        from_date: str = "2022-03-22",
        to_date: str = None,
        items_per_page: int = 100,
        region: str = "all",
        days_interval: int = 30,
        cols_to_drop: List[str] = None,
    ) -> pd.DataFrame:
        """
        Task run method.

        Args:
            source (Literal["jobs", "product", "company", "survey"], optional): The endpoint source to be accessed. Defaults to None.
            credentials (Dict[str, Any], optional): Stores the credentials information. Defaults to None.
            from_date (str, optional): Start date for the query, by default is the oldest date in the data, '2022-03-22'.
            to_date (str, optional): End date for the query. By default None, which will be executed as datetime.today().strftime("%Y-%m-%d") in code.
            items_per_page (int, optional): Number of entries per page. 100 entries by default.
            region (str, optional): Region filter for the query. Valid inputs: ["bg", "hu", "hr", "pl", "ro", "si", "all"]. Defaults to "all".
            days_interval (int, optional): Days specified in date range per api call (test showed that 30-40 is optimal for performance). Defaults to 30.
            cols_to_drop (List[str], optional): List of columns to drop. Defaults to None.

        Raises:
            KeyError: When DataFrame doesn't contain columns provided in the list of columns to drop.
            TypeError: When cols_to_drop is not a list type.

        Returns:
            pd.DataFrame: The query result as a pandas DataFrame.
        """

        vc_obj = VidClub(credentials=credentials)

        vc_dataframe = vc_obj.total_load(
            source=source,
            from_date=from_date,
            to_date=to_date,
            items_per_page=items_per_page,
            region=region,
            days_interval=days_interval,
        )
        if cols_to_drop is not None:
            if isinstance(cols_to_drop, list):
                try:
                    logger.info(f"Dropping following columns: {cols_to_drop}...")
                    vc_dataframe.drop(
                        columns=cols_to_drop, inplace=True, errors="raise"
                    )
                except KeyError as ke:
                    logger.error(
                        f"Column(s): {cols_to_drop} don't exist in the DataFrame. No columns were dropped. Returning full DataFrame..."
                    )
                    logger.info(f"Existing columns: {vc_dataframe.columns}")
            else:
                raise TypeError("Provide columns to drop in a List.")

        return vc_dataframe
