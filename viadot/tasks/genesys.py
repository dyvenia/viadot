import time
from typing import Any, Dict, List, Literal

import pandas as pd
import numpy as np
import prefect
from prefect import Task
from prefect.utilities import logging
from prefect.utilities.tasks import defaults_from_attrs

from ..exceptions import APIError
from ..sources import Genesys

logger = logging.get_logger()


class GenesysToCSV(Task):
    def __init__(
        self,
        report_name: str = "genesys_to_csv",
        view_type: str = "queue_performance_detail_view",
        media_type_list: List[str] = None,
        queueIds_list: List[str] = None,
        data_to_post_str: str = None,
        start_date: str = None,
        end_date: str = None,
        days_interval: int = 1,
        environment: str = None,
        schedule_id: str = None,
        report_url: str = None,
        report_columns: List[str] = None,
        credentials_genesys: Dict[str, Any] = None,
        timeout: int = 3600,
        *args: List[Any],
        **kwargs: Dict[str, Any],
    ):
        """Task for downloading data from Genesys API to CSV.

        Args:
            report_name (str, optional): The name of this task. Defaults to a general name 'genesys_to_csv'.
            view_type (str, optional): The type of view export job to be created. Defaults to "queue_performance_detail_view".
            media_type_list (List[str], optional): List of specific media types. Defaults to None.
            queueIds_list (List[str], optional): List of specific queues ids. Defaults to None.
            data_to_post_str (str, optional): String template to generate json body. Defaults to None.
            credentials_genesys (Dict[str, Any], optional): Credentials to connect with Genesys API containing CLIENT_ID. Defaults to None.
            start_date (str, optional): Start date of the report. Defaults to None.
            end_date (str, optional): End date of the report. Defaults to None.
            days_interval (int, optional): How many days report should include. Defaults to 1.
            environment (str, optional): Adress of host server. Defaults to None than will be used enviroment
            from credentials.
            schedule_id (str, optional): The ID of report. Defaults to None.
            report_url (str, optional): The url of report generated in json response. Defaults to None.
            report_columns (List[str], optional): List of exisiting column in report. Defaults to None.
            timeout(int, optional): The amount of time (in seconds) to wait while running this task before
                a timeout occurs. Defaults to 3600.
        """

        self.logger = prefect.context.get("logger")
        self.schedule_id = schedule_id
        self.report_name = report_name
        self.view_type = view_type
        self.environment = environment
        self.report_url = report_url
        self.report_columns = report_columns
        self.media_type_list = media_type_list
        self.queueIds_list = queueIds_list
        self.data_to_post_str = data_to_post_str
        self.start_date = start_date
        self.end_date = end_date
        self.days_interval = days_interval
        self.credentials_genesys = credentials_genesys

        super().__init__(
            name=self.report_name,
            timeout=timeout,
            *args,
            **kwargs,
        )

    def __call__(self, *args, **kwargs):
        """Download Genesys data to CSV"""
        return super().__call__(*args, **kwargs)

    @defaults_from_attrs(
        "report_name",
        "view_type",
        "environment",
        "schedule_id",
        "report_url",
        "media_type_list",
        "queueIds_list",
        "data_to_post_str",
        "start_date",
        "end_date",
        "report_columns",
        "days_interval",
        "credentials_genesys",
    )
    def run(
        self,
        report_name: str = None,
        view_type: str = "queue_performance_detail_view",
        view_type_time_sleep: int = 80,
        environment: str = None,
        schedule_id: str = None,
        report_url: str = None,
        media_type_list: List[str] = None,
        queueIds_list: List[str] = None,
        data_to_post_str: str = None,
        start_date: str = None,
        end_date: str = None,
        report_columns: List[str] = None,
        days_interval: int = None,
        credentials_genesys: Dict[str, Any] = None,
    ) -> List[str]:
        """
        Task for downloading data from the Genesys API to DF.

        Args:
            report_name (str, optional): The name of this task. Defaults to a general name 'genesys_to_csv'.
            view_type (str, optional): The type of view export job to be created. Defaults to "queue_performance_detail_view".
            view_type_time_sleep (int, optional): Waiting time to retrieve data from Genesys API. Defaults to 80.
            media_type_list (List[str], optional): List of specific media types. Defaults to None.
            queueIds_list (List[str], optional): List of specific queues ids. Defaults to None.
            data_to_post_str (str, optional): String template to generate json body. Defaults to None.
            credentials_genesys (Dict[str, Any], optional): Credentials to connect with Genesys API containing CLIENT_ID. Defaults to None.
            start_date (str, optional): Start date of the report. Defaults to None.
            end_date (str, optional): End date of the report. Defaults to None.
            days_interval (int, optional): How many days report should include. Defaults to 1.
            environment (str, optional): Adress of host server. Defaults to None than will be used enviroment
            from credentials.
            schedule_id (str, optional): The ID of report. Defaults to None.
            report_url (str, optional): The url of report generated in json response. Defaults to None.
            report_columns (List[str], optional): List of exisiting column in report. Defaults to None.

        Returns:
            List[str]: List of file names.
        """

        genesys = Genesys(
            report_name=report_name,
            view_type=view_type,
            media_type_list=media_type_list,
            queueIds_list=queueIds_list,
            data_to_post_str=data_to_post_str,
            credentials_genesys=credentials_genesys,
            start_date=start_date,
            end_date=end_date,
            days_interval=days_interval,
            environment=environment,
            schedule_id=schedule_id,
            report_url=report_url,
            report_columns=report_columns,
        )

        genesys.genesys_generate_body()
        genesys.genesys_generate_exports()

        if view_type == "queue_performance_detail_view":
            logger.info(
                f"Waiting {view_type_time_sleep} seconds for caching data in Genesys database."
            )
            # sleep time to allow Genesys generate all exports
            time.sleep(view_type_time_sleep)
            # in order to wait for API POST request add it
            timeout_start = time.time()
            # 30 seconds timeout is minimal but for safety added 60.
            timeout = timeout_start + 60
            # while loop with timeout
            while time.time() < timeout:

                try:
                    genesys.get_reporting_exports_data()
                    urls = [col for col in np.array(genesys.report_data).T][1]
                    if None in urls:
                        logger.warning("Found None object in list of urls.")
                    else:
                        break
                except TypeError:
                    pass

                # There is a need to clear a list before repeating try statement.
                genesys.report_data.clear()

        elif view_type in [
            "agent_performance_summary_view",
            "agent_status_summary_view",
        ]:
            logger.info(
                f"Waiting for getting data in Genesys database ({view_type_time_sleep} seconds)."
            )
            time.sleep(view_type_time_sleep)

            genesys.get_reporting_exports_data()
        # print(genesys.report_data)
        failed = [col for col in np.array(genesys.report_data).T][-1]

        if "FAILED" in failed and "COMPLETED" in failed:
            logger.warning("Some reports failed.")

        if len(genesys.report_data) == 0 or np.unique(failed)[0] == "FAILED":
            genesys.delete_all_reporting_exports()
            logger.warning(f"All existing reports were delted.")
            raise APIError("No exporting reports were generated.")
        elif not None in [col for col in np.array(genesys.report_data).T][1]:
            logger.info("Downloaded the data from the Genesys into the CSV.")
        else:
            logger.info("Succesfully loaded all exports.")

        file_names = genesys.download_all_reporting_exports()
        logger.info("Downloaded the data from the Genesys into the CSV.")
        # in order to wait for API GET request call it
        logger.info("Waiting for caching data in Genesys database.")
        time.sleep(20)
        genesys.delete_all_reporting_exports()
        logger.info(f"All existing reports were delted.")

        return file_names


class GenesysToDF(Task):
    def __init__(
        self,
        report_name: str = None,
        environment: str = None,
        schedule_id: str = None,
        report_url: str = None,
        report_columns: List[str] = None,
        credentials_genesys: Dict[str, Any] = None,
        timeout: int = 3600,
        *args: List[Any],
        **kwargs: Dict[str, Any],
    ):

        self.logger = prefect.context.get("logger")
        self.schedule_id = schedule_id
        self.report_name = report_name
        self.environment = environment
        self.report_url = report_url
        self.report_columns = report_columns
        self.credentials_genesys = credentials_genesys

        super().__init__(
            name="genesys_to_df",
            timeout=timeout,
            *args,
            **kwargs,
        )

    def __call__(self, *args, **kwargs):
        """Download Genesys data to DF"""
        return super().__call__(*args, **kwargs)

    @defaults_from_attrs(
        "report_name",
        "environment",
        "schedule_id",
        "report_url",
        "report_columns",
        "credentials_genesys",
    )
    def run(
        self,
        report_name: str = None,
        environment: str = None,
        schedule_id: str = None,
        report_url: str = None,
        report_columns: List[str] = None,
        credentials_genesys: Dict[str, Any] = None,
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
            credentials_genesys (Dict[str, Any], optional): Credentials to connect with Genesys API containing CLIENT_ID. Defaults to None.

        Returns:
            pd.DataFrame: The API GET as a pandas DataFrames from Genesys.
        """
        genesys = Genesys(
            report_name=report_name,
            credentials=credentials_genesys,
            environment=environment,
            schedule_id=schedule_id,
            report_url=report_url,
            report_columns=report_columns,
        )

        df = genesys.to_df()

        logger.info(f"Downloaded the data from the Genesys into the Data Frame.")
        return df
