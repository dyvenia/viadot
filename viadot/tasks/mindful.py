import time
import json
from typing import Any, Dict, List, Literal

from datetime import datetime, timedelta
from prefect import Task
from prefect.utilities import logging
from prefect.utilities.tasks import defaults_from_attrs

from viadot.sources import Mindful
from viadot.config import local_config
from viadot.tasks import AzureKeyVaultSecret
from viadot.exceptions import CredentialError

logger = logging.get_logger()


class MindfulToCSV(Task):
    def __init__(
        self,
        report_name: str = "mindful_to_csv",
        start_date: datetime = None,
        end_date: datetime = None,
        date_interval: int = 1,
        region: Literal["us1", "us2", "us3", "ca1", "eu1", "au1"] = "eu1",
        file_extension: Literal["parquet", "csv"] = "csv",
        file_path: str = "",
        timeout: int = 3600,
        *args: List[Any],
        **kwargs: Dict[str, Any],
    ):
        """Task for downloading data from Mindful API to CSV.

        Args:
            report_name (str, optional): The name of this task. Defaults to "mindful_to_csv".
            start_date (datetime, optional): Start date of the request. Defaults to None.
            end_date (datetime, optional): End date of the resquest. Defaults to None.
            date_interval (int, optional): How many days are included in the request.
                If end_date is passed as an argument, date_interval will be invalidated. Defaults to 1.
            region (Literal[us1, us2, us3, ca1, eu1, au1], optional): SD region from where to interact with the mindful API. Defaults to "eu1".
            file_extension (Literal[parquet, csv], optional): File extensions for storing responses. Defaults to "csv".
            file_path (str, optional): Path where to save the file locally. Defaults to ''.
            timeout(int, optional): The amount of time (in seconds) to wait while running this task before
                a timeout occurs. Defaults to 3600.

        Raises:
            CredentialError: If credentials are not provided in local_config or directly as a parameter inside run method.
        """

        self.start_date = start_date
        self.end_date = end_date
        self.date_interval = date_interval
        self.region = region
        self.file_extension = file_extension
        self.file_path = file_path

        super().__init__(
            name=report_name,
            timeout=timeout,
            *args,
            **kwargs,
        )

        if not isinstance(start_date, datetime):
            self.start_date = datetime.now() - timedelta(days=date_interval)
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
        "region",
        "file_extension",
        "file_path",
    )
    def run(
        self,
        credentials_mindful: Dict[str, Any] = None,
        credentials_secret: str = None,
        vault_name: str = None,
        start_date: datetime = None,
        end_date: datetime = None,
        date_interval: int = 1,
        file_extension: Literal["parquet", "csv"] = "csv",
        region: Literal["us1", "us2", "us3", "ca1", "eu1", "au1"] = "eu1",
        file_path: str = "",
    ):

        if credentials_mindful is not None:
            self.logger.info("Mindful credentials provided by user")
        elif credentials_mindful is None and credentials_secret is not None:
            credentials_str = AzureKeyVaultSecret(
                credentials_secret, vault_name=vault_name
            ).run()
            credentials_mindful = json.loads(credentials_str)
            logger.info("Loaded credentials from Key Vault.")
        else:
            try:
                credentials_mindful = local_config["MINDFUL"]
                self.logger.info("Mindful credentials loaded from local config")
            except KeyError:
                credentials_mindful = None
                raise CredentialError("Credentials not found.")

        header = {
            "Authorization": f"Bearer {credentials_mindful.get('VAULT')}",
        }

        mindful = Mindful(
            header=header,
            region=region,
            start_date=start_date,
            end_date=end_date,
            date_interval=date_interval,
            file_extension=file_extension,
        )

        file_names = []
        # interactions
        interactions_response = mindful.get_interactions_list()
        if interactions_response.status_code == 200:
            interaction_file_name = mindful.response_to_file(
                interactions_response,
                file_path=file_path,
            )
            file_names.append(interaction_file_name)
            logger.info(
                "Successfully downloaded interactions data from the Mindful API."
            )
            time.sleep(0.5)

        # responses
        responses_response = mindful.get_responses_list()
        if responses_response.status_code == 200:
            response_file_name = mindful.response_to_file(
                responses_response,
                file_path=file_path,
            )
            file_names.append(response_file_name)
            logger.info("Successfully downloaded responses data from the Mindful API.")
            time.sleep(0.5)

        # surveys
        surveys_response = mindful.get_survey_list()
        if surveys_response.status_code == 200:
            surveys_file_name = mindful.response_to_file(
                surveys_response,
                file_path=file_path,
            )
            file_names.append(surveys_file_name)
            logger.info("Successfully downloaded surveys data from the Mindful API.")

        if not file_names:
            return None
        else:
            return file_names
