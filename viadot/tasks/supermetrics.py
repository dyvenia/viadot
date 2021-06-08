from datetime import timedelta
from typing import Any, Dict, List, Union

from prefect import Task
from prefect.utilities.tasks import defaults_from_attrs

from ..sources import Supermetrics


class SupermetricsToCSV(Task):
    def __init__(
        self,
        *args,
        max_retries: int = 5,
        retry_delay: timedelta = timedelta(seconds=10),
        timeout: int = 60 * 30,
        max_rows: int = 1_000_000,
        if_exists: str = "replace",
        if_empty: str = "warn",
        sep="\t",
        **kwargs,
    ):
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

    @defaults_from_attrs(
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
