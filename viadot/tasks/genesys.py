import time
from typing import Any, Dict, List

import pandas as pd
import prefect
from prefect import Task
from prefect.utilities import logging
from prefect.utilities.tasks import defaults_from_attrs

from viadot.config import local_config
from ..exceptions import CredentialError
from ..sources import Genesys

logger = logging.get_logger()


class GenesysToCSV(Task):
    def __init__(
        self,
        report_name: str = "genesys_to_csv",
        media_type_list: List[str] = None,
        queueIds_list: List[str] = None,
        data_to_post_str: str = None,
        credentials: Dict[str, Any] = None,
        start_date: str = None,
        end_date: str = None,
        days_interval: int = 1,
        environment: str = None,
        schedule_id: str = None,
        report_url: str = None,
        report_columns: List[str] = None,
        *args: List[Any],
        **kwargs: Dict[str, Any],
    ):
        """_summary_

        Args:
            report_name (str, optional): The name of this task. Defaults to a general name 'genesys_to_csv'.
            media_type_list (List[str], optional): List of specific media types. Defaults to None.
            queueIds_list (List[str], optional): List of specific queues ids. Defaults to None.
            data_to_post_str (str, optional): String template to generate json body. Defaults to None.
            credentials (Dict[str, Any], optional): Credentials to connect with Genesys API containing CLIENT_ID. Defaults to None.
            start_date (str, optional): Start date of the report. Defaults to None.
            end_date (str, optional): End date of the report. Defaults to None.
            days_interval (int, optional): How many days report should include. Defaults to 1.
            environment (str, optional): Adress of host server. Defaults to None than will be used enviroment
            from credentials.
            schedule_id (str, optional): The ID of report. Defaults to None.
            report_url (str, optional): The url of report generated in json response. Defaults to None.
            report_columns (List[str], optional): List of exisiting column in report. Defaults to None.

        Raises:
            CredentialError: If credentials are not provided in local_config or directly as a parameter.
        """

        try:
            DEFAULT_CREDENTIALS = local_config["GENESYS"]
        except KeyError:
            DEFAULT_CREDENTIALS = None
        self.credentials = credentials or DEFAULT_CREDENTIALS
        if self.credentials is None:
            raise CredentialError("Credentials not found.")

        self.logger = prefect.context.get("logger")
        self.schedule_id = schedule_id
        self.report_name = report_name
        self.environment = environment
        self.report_url = report_url
        self.report_columns = report_columns
        self.media_type_list = media_type_list
        self.queueIds_list = queueIds_list
        self.data_to_post_str = data_to_post_str
        self.start_date = start_date
        self.end_date = end_date
        self.days_interval = days_interval

        # Get schedule id to retrive report url
        if self.schedule_id is None:
            SCHEDULE_ID = self.credentials.get("SCHEDULE_ID", None)
            if SCHEDULE_ID is not None:
                self.schedule_id = SCHEDULE_ID

        if self.environment is None:
            self.environment = self.credentials.get("ENVIRONMENT", None)

        super().__init__(
            name=self.report_name,
            *args,
            **kwargs,
        )

    def __call__(self, *args, **kwargs):
        """Download Genesys data to DF"""
        return super().__call__(*args, **kwargs)

    @defaults_from_attrs(
        "report_name", "environment", "schedule_id", "report_url", "report_columns"
    )
    def run(
        self,
        report_name: str = None,
        environment: str = None,
        schedule_id: str = None,
        report_url: str = None,
        report_columns: List[str] = None,
    ) -> pd.DataFrame:
        """
        Task for downloading data from the Genesys API to DF.

        Args:
            report_name (str, optional): Name of the report. Defaults to None.
            environment (str, optional): Adress of host server. Defaults to None than will be used enviroment
            from credentials.
            schedule_id (str, optional): The ID of report. Defaults to None.
            report_url (str, optional): The url of report generated in json response. Defaults to None.
            report_columns (List[str], optional): List of exisiting column in report. Defaults to None.

        Returns:
            pd.DataFrame: The API GET as a pandas DataFrames from Genesys.
        """
        genesys = Genesys(
            report_name=report_name,
            media_type_list=self.media_type_list,
            queueIds_list=self.queueIds_list,
            data_to_post_str=self.data_to_post_str,
            credentials=self.credentials,
            start_date=self.start_date,
            end_date=self.end_date,
            days_interval=self.days_interval,
            environment=environment,
            schedule_id=schedule_id,
            report_url=report_url,
            report_columns=report_columns,
        )
        genesys.genesys_generate_body()
        genesys.genesys_generate_exports()
        logger.info(f"Waiting for caching data in Genesys database.")
        # in order to wait for API POST request add it
        time.sleep(50)
        genesys.get_reporting_exports_data()
        file_names = genesys.download_all_reporting_exports()
        logger.info(f"Downloaded the data from the Genesys into the CSV.")
        # in order to wait for API GET request call it
        logger.info(f"Waiting for caching data in Genesys database.")
        time.sleep(20)
        genesys.delete_all_reporting_exports()
        logger.info(f"All existing reports were delted.")

        return file_names


# ! old version
class GenesysToDF(Task):
    def __init__(
        self,
        report_name: str = None,
        credentials: Dict[str, Any] = None,
        environment: str = None,
        schedule_id: str = None,
        report_url: str = None,
        report_columns: List[str] = None,
        *args: List[Any],
        **kwargs: Dict[str, Any],
    ):

        try:
            DEFAULT_CREDENTIALS = local_config["GENESYS"]
        except KeyError:
            DEFAULT_CREDENTIALS = None
        self.credentials = credentials or DEFAULT_CREDENTIALS
        if self.credentials is None:
            raise CredentialError("Credentials not found.")

        self.logger = prefect.context.get("logger")
        self.schedule_id = schedule_id
        self.report_name = report_name
        self.environment = environment
        self.report_url = report_url
        self.report_columns = report_columns

        # Get schedule id to retrive report url
        if self.schedule_id is None:
            SCHEDULE_ID = self.credentials.get("SCHEDULE_ID", None)
            if SCHEDULE_ID is not None:
                self.schedule_id = SCHEDULE_ID

        if self.environment is None:
            self.environment = self.credentials.get("ENVIRONMENT", None)

        super().__init__(
            name="genesys_to_df",
            *args,
            **kwargs,
        )

    def __call__(self, *args, **kwargs):
        """Download Genesys data to DF"""
        return super().__call__(*args, **kwargs)

    @defaults_from_attrs(
        "report_name", "environment", "schedule_id", "report_url", "report_columns"
    )
    def run(
        self,
        report_name: str = None,
        environment: str = None,
        schedule_id: str = None,
        report_url: str = None,
        report_columns: List[str] = None,
    ) -> pd.DataFrame:
        """
        Task for downloading data from the Genesys API to DF.

        Args:
            report_name (str, optional): Name of the report. Defaults to None.
            environment (str, optional): Adress of host server. Defaults to None than will be used enviroment
            from credentials.
            schedule_id (str, optional): The ID of report. Defaults to None.
            report_url (str, optional): The url of report generated in json response. Defaults to None.
            report_columns (List[str], optional): List of exisiting column in report. Defaults to None.

        Returns:
            pd.DataFrame: The API GET as a pandas DataFrames from Genesys.
        """
        genesys = Genesys(
            report_name=report_name,
            credentials=self.credentials,
            environment=environment,
            schedule_id=schedule_id,
            report_url=report_url,
            report_columns=report_columns,
        )

        df = genesys.to_df()

        logger.info(f"Downloaded the data from the Genesys into the Data Frame.")
        return df
