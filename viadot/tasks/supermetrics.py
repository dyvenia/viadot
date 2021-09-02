from datetime import timedelta
from typing import Any, Dict, List, Union

import pandas as pd
from prefect import Task
from prefect.utilities.tasks import defaults_from_attrs

from ..sources import Supermetrics


class SupermetricsToCSV(Task):
    def __init__(
        self,
        *args,
        path: str = "supermetrics_extract.csv",
        max_retries: int = 5,
        retry_delay: timedelta = timedelta(seconds=10),
        timeout: int = 60 * 30,
        max_rows: int = 1_000_000,
        if_exists: str = "replace",
        if_empty: str = "warn",
        sep="\t",
        **kwargs,
    ):
        self.path = path
        self.max_rows = max_rows
        self.if_exists = if_exists
        self.if_empty = if_empty
        self.sep = sep

        super().__init__(
            name="supermetrics_to_csv",
            max_retries=max_retries,
            retry_delay=retry_delay,
            timeout=timeout,
            *args,
            **kwargs,
        )

    def __call__(self):
        """Download Supermetrics data to a CSV"""
        super().__call__(self)

    @defaults_from_attrs(
        "path",
        "max_rows",
        "if_exists",
        "if_empty",
        "max_retries",
        "retry_delay",
        "timeout",
        "sep",
    )
    def run(
        self,
        path: str = None,
        ds_id: str = None,
        ds_accounts: Union[str, List[str]] = None,
        ds_segments: List[str] = None,
        ds_user: str = None,
        fields: List[str] = None,
        date_range_type: str = None,
        start_date: str = None,
        end_date: str = None,
        settings: Dict[str, Any] = None,
        filter: str = None,
        max_rows: int = None,
        max_columns: int = None,
        order_columns: str = None,
        if_exists: str = None,
        if_empty: str = None,
        max_retries: int = None,
        retry_delay: timedelta = None,
        timeout: int = None,
        sep: str = None,
    ):

        if max_retries:
            self.max_retries = max_retries

        if retry_delay:
            self.retry_delay = retry_delay

        if isinstance(ds_accounts, str):
            ds_accounts = [ds_accounts]

        # Build the URL
        # Note the task accepts only one account per query
        query = dict(
            ds_id=ds_id,
            ds_accounts=ds_accounts,
            ds_segments=ds_segments,
            ds_user=ds_user,
            fields=fields,
            date_range_type=date_range_type,
            start_date=start_date,
            end_date=end_date,
            settings=settings,
            filter=filter,
            max_rows=max_rows,
            max_columns=max_columns,
            order_columns=order_columns,
        )
        query = {param: val for param, val in query.items() if val is not None}
        supermetrics = Supermetrics()
        supermetrics.query(query)

        # Download data to a local CSV file
        self.logger.info(f"Downloading data to {path}...")
        supermetrics.to_csv(path, if_exists=if_exists, if_empty=if_empty, sep=sep)
        self.logger.info(f"Successfully downloaded data to {path}.")


class SupermetricsToDF(Task):
    """
    Task for downloading data from the Supermetrics API to a pandas DataFrame.

    Args:
        ds_id (str, optional): A Supermetrics query parameter.
        ds_accounts (Union[str, List[str]], optional): A Supermetrics query parameter. Defaults to None.
        ds_segments (List[str], optional): A Supermetrics query parameter. Defaults to None.
        ds_user (str, optional): A Supermetrics query parameter. Defaults to None.
        fields (List[str], optional): A Supermetrics query parameter. Defaults to None.
        date_range_type (str, optional): A Supermetrics query parameter. Defaults to None.
        settings (Dict[str, Any], optional): A Supermetrics query parameter. Defaults to None.
        filter (str, optional): A Supermetrics query parameter. Defaults to None.
        max_rows (int, optional): A Supermetrics query parameter. Defaults to None.
        max_columns (int, optional): A Supermetrics query parameter. Defaults to None.
        order_columns (str, optional): A Supermetrics query parameter. Defaults to None.
        if_empty (str, optional): What to do if query returns no data. Defaults to "warn".
        max_retries (int, optional): The maximum number of retries. Defaults to 5.
        retry_delay (timedelta, optional): The delay between task retries. Defaults to 10 seconds.
        timeout (int, optional): Task timeout. Defaults to 30 minuntes.
    """

    def __init__(
        self,
        *args,
        if_empty: str = "warn",
        max_rows: int = 1_000_000,
        max_retries: int = 5,
        retry_delay: timedelta = timedelta(seconds=10),
        timeout: int = 60 * 30,
        **kwargs,
    ):

        self.if_empty = if_empty
        self.max_rows = max_rows

        super().__init__(
            name="supermetrics_to_df",
            max_retries=max_retries,
            retry_delay=retry_delay,
            timeout=timeout,
            *args,
            **kwargs,
        )

    @defaults_from_attrs(
        "if_empty",
        "max_rows",
        "max_retries",
        "retry_delay",
        "timeout",
    )
    def run(
        self,
        ds_id: str = None,
        ds_accounts: Union[str, List[str]] = None,
        ds_segments: List[str] = None,
        ds_user: str = None,
        fields: List[str] = None,
        date_range_type: str = None,
        start_date: str = None,
        end_date: str = None,
        settings: Dict[str, Any] = None,
        filter: str = None,
        max_rows: int = None,
        max_columns: int = None,
        order_columns: str = None,
        if_empty: str = None,
        max_retries: int = None,
        retry_delay: timedelta = None,
        timeout: int = None,
    ) -> pd.DataFrame:
        """
        Task run method.

        Args:
            ds_id (str, optional): A Supermetrics query parameter.
            ds_accounts (Union[str, List[str]], optional): A Supermetrics query parameter. Defaults to None.
            ds_segments (List[str], optional): A Supermetrics query parameter. Defaults to None.
            ds_user (str, optional): A Supermetrics query parameter. Defaults to None.
            fields (List[str], optional): A Supermetrics query parameter. Defaults to None.
            date_range_type (str, optional): A Supermetrics query parameter. Defaults to None.
            settings (Dict[str, Any], optional): A Supermetrics query parameter. Defaults to None.
            filter (str, optional): A Supermetrics query parameter. Defaults to None.
            max_rows (int, optional): A Supermetrics query parameter. Defaults to None.
            max_columns (int, optional): A Supermetrics query parameter. Defaults to None.
            order_columns (str, optional): A Supermetrics query parameter. Defaults to None.
            if_empty (str, optional): What to do if query returns no data. Defaults to "warn".
            max_retries (int, optional): The maximum number of retries. Defaults to 5.
            retry_delay (timedelta, optional): The delay between task retries. Defaults to 10 seconds.
            timeout (int, optional): Task timeout. Defaults to 30 minuntes.

        Returns:
            pd.DataFrame: The query result as a pandas DataFrame.
        """

        if max_retries:
            self.max_retries = max_retries

        if retry_delay:
            self.retry_delay = retry_delay

        if isinstance(ds_accounts, str):
            ds_accounts = [ds_accounts]

        # Build the URL
        # Note the task accepts only one account per query
        query = dict(
            ds_id=ds_id,
            ds_accounts=ds_accounts,
            ds_segments=ds_segments,
            ds_user=ds_user,
            fields=fields,
            date_range_type=date_range_type,
            start_date=start_date,
            end_date=end_date,
            settings=settings,
            filter=filter,
            max_rows=max_rows,
            max_columns=max_columns,
            order_columns=order_columns,
        )
        query = {param: val for param, val in query.items() if val is not None}
        supermetrics = Supermetrics()
        supermetrics.query(query)

        # Download data to a local CSV file
        self.logger.info(f"Downloading data to a DataFrame...")
        df = supermetrics.to_df(if_empty=if_empty)
        self.logger.info(f"Successfully downloaded data to a DataFrame.")
        return df
