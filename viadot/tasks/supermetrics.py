from datetime import timedelta
from typing import Any, Dict

import prefect
from prefect import Task

from ..sources import Supermetrics


class SupermetricsToCSV(Task):
    def __init__(self,  *args, max_retries: int = 5, retry_delay: timedelta = timedelta(seconds=10), **kwargs):
        super().__init__(
            name="supermetrics_to_csv",
            max_retries=max_retries,
            retry_delay=retry_delay,
            *args,
            **kwargs,
        )

    def __call__(self):
        """Download Supermetrics data to a CSV"""

    def run(self, query: Dict[str, Any], path: str, max_retries: int = 5, retry_delay: timedelta = timedelta(seconds=10)):

        logger = prefect.context.get("logger")

        # Build the URL
        supermetrics = Supermetrics()
        supermetrics.query(query)

        # Download data to a local CSV file
        logger.info(f"Downloading data to {path}...")
        supermetrics.to_csv(path)
        logger.info(f"Successfully downloaded data to {path}.")
