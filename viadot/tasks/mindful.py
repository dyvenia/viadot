import time
from typing import Any, Dict, List, Literal

from datetime import datetime, timedelta
from prefect import Task
from prefect.utilities import logging
from prefect.utilities.tasks import defaults_from_attrs

from viadot.sources import Mindful

logger = logging.get_logger()


class MindfulToCSV(Task):
    def __init__(
        self,
        report_name: str = "mindful_to_csv",
        credentials_mindful: Dict[str, Any] = None,
        start_date: datetime = None,
        end_date: datetime = None,
        date_interval: int = 1,
        file_extension: Literal["parquet", "csv"] = "csv",
        file_path: str = "",
        *args: List[Any],
        **kwargs: Dict[str, Any],
    ):
        """Task for downloading data from Mindful API to CSV.

        Args:
            report_name (str, optional): The name of this task. Defaults to "mindful_to_csv".
            credentials_mindful (Dict[str, Any], optional): Credentials to connect with Mindful API. Defaults to None.
            start_date (datetime, optional): Start date of the request. Defaults to None.
            end_date (datetime, optional): End date of the resquest. Defaults to None.
            date_interval (int, optional): How many days are included in the request.
                If end_date is passed as an argument, date_interval will be invalidated. Defaults to 1.
            file_extension (Literal[parquet, csv], optional): file extensions for storing responses. Defaults to "csv".
            file_path (str, optional): Path where to save the file locally. Defaults to ''.
        """
        self.credentials_mindful = credentials_mindful
        self.start_date = start_date
        self.end_date = end_date
        self.date_interval = date_interval
        self.file_extension = file_extension
        self.file_path = file_path

        super().__init__(
            name=report_name,
            *args,
            **kwargs,
        )

        if not isinstance(start_date, datetime):
            self.start_date = datetime.now()
            self.end_date = self.start_date + timedelta(days=date_interval)
        elif isinstance(start_date, datetime) and not isinstance(end_date, datetime):
            self.start_date = start_date
            self.end_date = start_date + timedelta(days=date_interval)
            if self.end_date > datetime.now():
                self.end_date = datetime.now()
        elif start_date >= end_date:
            raise ValueError(
                f"start_date variable must be lower than end_date variable."
            )
        else:
            self.start_date = start_date
            self.end_date = end_date

    def __call__(self, *args, **kwargs):
        """Download Mindful data to CSV"""
        return super().__call__(*args, **kwargs)

    @defaults_from_attrs(
        "start_date",
        "end_date",
        "date_interval",
        "credentials_mindful",
        "file_extension",
        "file_path",
    )
    def run(
        self,
        credentials_mindful: Dict[str, Any] = None,
        start_date: datetime = None,
        end_date: datetime = None,
        date_interval: int = 1,
    ):
        mindful = Mindful(
            credentials_mindful=credentials_mindful,
            region="eu1",
            start_date=start_date,
            end_date=end_date,
            date_interval=date_interval,
            file_extension=self.file_extension,
        )

        file_paths = []
        interactions_response = mindful.get_interactions_list()
        interaction_file_path = mindful.response_to_file(
            interactions_response,
            file_name=f"mindful_interactions_{end_date.year}_{end_date.month:02}_{end_date.day:02}",
            file_path=self.file_path,
        )
        file_paths.append(interaction_file_path)
        logger.info("Successfully downloaded interactions data from the Mindful API.")
        time.sleep(0.5)
        responses_response = mindful.get_responses_list()
        response_file_path = mindful.response_to_file(
            responses_response,
            file_name=f"mindful_responses_{end_date.year}_{end_date.month:02}_{end_date.day:02}",
            file_path=self.file_path,
        )
        file_paths.append(response_file_path)
        logger.info("Successfully downloaded responses data from the Mindful API.")

        return file_paths
