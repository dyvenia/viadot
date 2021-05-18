from datetime import timedelta
from typing import Any, Dict, List

import prefect
from prefect import Task

from ..sources import Supermetrics


class SupermetricsToCSV(Task):
    def __init__(
        self,
        *args,
        max_retries: int = 5,
        retry_delay: timedelta = timedelta(seconds=10),
        **kwargs,
    ):
        super().__init__(
            name="supermetrics_to_csv",
            max_retries=max_retries,
            retry_delay=retry_delay,
            *args,
            **kwargs,
        )

    def __call__(self):
        """Download Supermetrics data to a CSV"""

    def run(
        self,
        path: str,
        ds_id: str,
        ds_account: str,
        ds_user: str,
        fields: List[str],
        date_range_type: str = None,
        settings: Dict[str, Any] = None,
        filter: str = None,
        max_rows: int = 1000000,
        if_exists: str = "replace",
        if_empty: str = "warn",
        max_retries: int = 5,
        retry_delay: timedelta = timedelta(seconds=10),
    ):
        logger = prefect.context.get("logger")

        if max_retries:
            self.max_retries = max_retries

        if retry_delay:
            self.retry_delay = retry_delay

        # Build the URL
        # Note the task accepts only one account per query due to low reliability of Supermetrics
        query = dict(
            ds_id=ds_id,
            ds_accounts=[ds_account],
            ds_user=ds_user,
            fields=fields,
            date_range_type=date_range_type,
            settings=settings,
            filter=filter,
            max_rows=max_rows,
        )
        query = {param: val for param, val in query.items() if val is not None}
        supermetrics = Supermetrics()
        supermetrics.query(query)

        # Download data to a local CSV file
        logger.info(f"Downloading data to {path}...")
        supermetrics.to_csv(path, if_exists=if_exists, if_empty=if_empty)
        logger.info(f"Successfully downloaded data to {path}.")
