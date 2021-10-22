import datetime
import pandas as pd

import prefect
from prefect import Task

from viadot.sources import UKCarbonIntensity


class StatsToDF(Task):
    """A Prefect task for downloading UK Carbon Instensity Statistics (stats) to a pandas DataFrame."""

    def __init__(self, *args, **kwargs):
        """Generate the task."""
        super().__init__(name="uk_carbon_intensity_stats_to_df", *args, **kwargs)

    def __call__(self):
      

    def run(self, df: pd.DataFrame, days_back: int = 1):
        """
        Run the task.

        Parameters
        ----------
        df : pd.DataFrame
            DataFrame created or edited by this task.
        days_back : int, optional
            How many days of stats to download.
            UK Carbon Intensity statistics are available for up to 30 days,
            by default previous day.
        """

        logger = prefect.context.get("logger")
        carbon = UKCarbonIntensity()
        now = datetime.datetime.now()
        dfs =[]
        logger.info(f"Downloading data to {df}...")
        for i in range(days_back):
            from_delta = datetime.timedelta(days=i + 1)
            to_delta = datetime.timedelta(days=i)
            to = now - to_delta
            from_ = now - from_delta
            carbon.query(f"/intensity/stats/{from_.isoformat()}/{to.isoformat()}")
            dfs.append(carbon.to_df())
            pd.concat(dfs)
            df.append

        # Download data to pandas.DataFrame
        logger.info(f"Successfully downloaded data to {df}.")


