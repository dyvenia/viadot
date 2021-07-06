import datetime

import prefect
from prefect import Task

from viadot.sources import UKCarbonIntensity


class StatsToCSV(Task):
    """A Prefect task for downloading UK Carbon Instensity Statistics (stats) to a csv file."""

    def __init__(self, *args, **kwargs):
        """Generate the task."""
        super().__init__(name="uk_carbon_intensity_stats_to_csv", *args, **kwargs)

    def __call__(self):
        """
        Run the task.

        Parameters
        ----------
        path : str
            Path of the csv file created or edited by this task
        days_back : int, optional
            How many days of stats to download in the csv.
            UK Carbon Intensity statistics are available for up to 30 days,
            by default 10
        """

    def run(self, path: str, days_back: int = 10):
        """
        Run the task.

        Parameters
        ----------
        path : str
            Path of the csv file created or edited by this task
        days_back : int, optional
            How many days of stats to download in the csv.
            UK Carbon Intensity statistics are available for up to 30 days,
            by default 10
        """

        logger = prefect.context.get("logger")
        carbon = UKCarbonIntensity()
        now = datetime.datetime.now()
        logger.info(f"Downloading data to {path}...")
        for i in range(days_back):
            from_delta = datetime.timedelta(days=i + 1)
            to_delta = datetime.timedelta(days=i)
            to = now - to_delta
            from_ = now - from_delta
            carbon.query(f"/intensity/stats/{from_.isoformat()}/{to.isoformat()}")
            carbon.to_csv(path, if_exists="append")

        # Download data to a local CSV file
        logger.info(f"Successfully downloaded data to {path}.")


class StatsToExcel(Task):
    """A Prefect task for downloading UK Carbon Instensity Statistics (stats) to a excel file."""

    def __init__(self, *args, **kwargs):
        """Generate the task."""
        super().__init__(name="uk_carbon_intensity_stats_to_excel", *args, **kwargs)

    def __call__(self):
        """
        Run the task.

        Parameters
        ----------
        path : str
            Path of the csv file created or edited by this task
        days_back : int, optional
            How many days of stats to download in the excel.
            UK Carbon Intensity statistics are available for up to 30 days,
            by default 10
        """

    def run(self, path: str, days_back: int = 10):
        """
        Run the task.

        Parameters
        ----------
        path : str
            Path of the excel file created or edited by this task
        days_back : int, optional
            How many days of stats to download in the excel.
            UK Carbon Intensity statistics are available for up to 30 days,
            by default 10
        """

        logger = prefect.context.get("logger")
        carbon = UKCarbonIntensity()
        now = datetime.datetime.now()
        logger.info(f"Downloading data to {path}...")
        for i in range(days_back):
            from_delta = datetime.timedelta(days=i + 1)
            to_delta = datetime.timedelta(days=i)
            to = now - to_delta
            from_ = now - from_delta
            carbon.query(f"/intensity/stats/{from_.isoformat()}/{to.isoformat()}")
            carbon.to_excel(path, if_exists="append")

        # Download data to a local excel file
        logger.info(f"Successfully downloaded data to {path}.")
