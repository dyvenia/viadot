import time
import sys
from typing import Any, Dict, List

import numpy as np
import pandas as pd
from pandas import DataFrame
import prefect
from prefect import Task
from prefect.engine import signals
from prefect.utilities import logging
from prefect.utilities.tasks import defaults_from_attrs

from viadot.exceptions import APIError
from viadot.sources import Genesys

logger = logging.get_logger()


class GenesysToCSV(Task):
    def __init__(
        self,
        report_name: str = "genesys_to_csv",
        view_type: str = "queue_performance_detail_view",
        post_data_list: List[str] = None,
        start_date: str = None,
        end_date: str = None,
        environment: str = None,
        schedule_id: str = None,
        report_url: str = None,
        report_columns: List[str] = None,
        local_file_path: str = "",
        credentials_genesys: Dict[str, Any] = None,
        timeout: int = 3600,
        *args: List[Any],
        **kwargs: Dict[str, Any],
    ):
        """Task for downloading data from Genesys API to CSV.

        Args:
            report_name (str, optional): The name of this task. Defaults to a general name 'genesys_to_csv'.
            view_type (str, optional): The type of view export job to be created. Defaults to "queue_performance_detail_view".
            post_data_list (List[str], optional): List of string templates to generate json body. Defaults to None.
            credentials_genesys (Dict[str, Any], optional): Credentials to connect with Genesys API containing CLIENT_ID. Defaults to None.
            start_date (str, optional): Start date of the report. Defaults to None.
            end_date (str, optional): End date of the report. Defaults to None.
            environment (str, optional): Adress of host server. Defaults to None than will be used enviroment
            from credentials.
            schedule_id (str, optional): The ID of report. Defaults to None.
            report_url (str, optional): The url of report generated in json response. Defaults to None.
            report_columns (List[str], optional): List of exisiting column in report. Defaults to None.
            local_file_path (str, optional): The local path from which to upload the file(s). Defaults to "".
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
        self.post_data_list = post_data_list
        self.start_date = start_date
        self.end_date = end_date
        self.credentials_genesys = credentials_genesys
        self.local_file_path = local_file_path

        super().__init__(
            name=self.report_name,
            timeout=timeout,
            *args,
            **kwargs,
        )

    def __call__(self, *args, **kwargs):
        """Download Genesys data to CSV"""
        return super().__call__(*args, **kwargs)

    def merge_nested_df(self, data_to_merge: list) -> DataFrame:
        # LEVEL 1
        df1 = pd.json_normalize(
            data_to_merge,
            record_path=["participants"],
            meta=[
                "conversationStart",
                "conversationEnd",
                "conversationId",
                "divisionIds",
                "mediaStatsMinConversationMos",
                "mediaStatsMinConversationRFactor",
                "originatingDirection",
            ],
            errors="ignore",
        )
        df1 = df1.drop(["sessions"], axis=1)

        # LEVEL 2
        df2 = pd.json_normalize(
            data_to_merge,
            record_path=["participants", "sessions"],
            meta=[
                ["participants", "externalContactId"],
                ["participants", "participantId"],
            ],
            errors="ignore",
        )
        df2 = df2.rename(
            columns={
                "participants.externalContactId": "externalContactId",
                "participants.participantId": "participantId",
            }
        )
        df2 = df2.drop(["metrics", "segments"], axis=1)

        # LEVEL 3
        df3_1 = pd.json_normalize(
            data_to_merge,
            record_path=["participants", "sessions", "metrics"],
            meta=[
                ["participants", "sessions", "sessionId"],
            ],
            errors="ignore",
        )
        df3_1 = df3_1.rename(columns={"participants.sessions.sessionId": "sessionId"})
        df3_2 = pd.json_normalize(
            data_to_merge,
            record_path=["participants", "sessions", "segments"],
            meta=[
                ["participants", "sessions", "sessionId"],
            ],
            errors="ignore",
        )
        df3_2 = df3_2.rename(columns={"participants.sessions.sessionId": "sessionId"})
        df3_3 = pd.json_normalize(
            data_to_merge,
            record_path=["participants", "sessions", "mediaEndpointStats"],
            meta=[
                ["participants", "sessions", "sessionId"],
            ],
            errors="ignore",
        )
        df3_3 = df3_3.rename(columns={"participants.sessions.sessionId": "sessionId"})

        # merging all the levels in a single data frame
        dff3 = pd.merge(df3_1, df3_2, df3_3, how="outer", on=["sessionId"])
        dff2 = pd.merge(df2, dff3, how="outer", on=["sessionId"])
        dff = pd.merge(
            df1, dff2, how="outer", on=["externalContactId", "participantId"]
        )

        return dff

    @defaults_from_attrs(
        "report_name",
        "view_type",
        "environment",
        "schedule_id",
        "report_url",
        "post_data_list",
        "start_date",
        "end_date",
        "report_columns",
        "credentials_genesys",
    )
    def run(
        self,
        report_name: str = None,
        view_type: str = None,
        view_type_time_sleep: int = 80,
        environment: str = None,
        schedule_id: str = None,
        report_url: str = None,
        post_data_list: List[str] = None,
        end_point: str = "reporting/exports",
        start_date: str = None,
        end_date: str = None,
        report_columns: List[str] = None,
        credentials_genesys: Dict[str, Any] = None,
    ) -> List[str]:
        """
        Task for downloading data from the Genesys API to DF.

        Args:
            report_name (str, optional): The name of this task. Defaults to a general name 'genesys_to_csv'.
            view_type (str, optional): The type of view export job to be created. Defaults to None.
            view_type_time_sleep (int, optional): Waiting time to retrieve data from Genesys API. Defaults to 80.
            post_data_list (List[str], optional): List of string templates to generate json body. Defaults to None.
            end_point (str, optional): Final end point for Genesys connection. Defaults to "reporting/exports".
            credentials_genesys (Dict[str, Any], optional): Credentials to connect with Genesys API containing CLIENT_ID. Defaults to None.
            start_date (str, optional): Start date of the report. Defaults to None.
            end_date (str, optional): End date of the report. Defaults to None.
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
            credentials_genesys=credentials_genesys,
            start_date=start_date,
            end_date=end_date,
            environment=environment,
            schedule_id=schedule_id,
            report_url=report_url,
            report_columns=report_columns,
        )

        if view_type == "queue_performance_detail_view":
            genesys.genesys_generate_exports(
                post_data_list=post_data_list, end_point=end_point
            )

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
            "agent_status_detail_view",
        ]:
            genesys.genesys_generate_exports(
                post_data_list=post_data_list, end_point=end_point
            )
            logger.info(
                f"Waiting for getting data in Genesys database ({view_type_time_sleep} seconds)."
            )
            time.sleep(view_type_time_sleep)

            genesys.get_reporting_exports_data()

        if view_type is not None:
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

            file_names = genesys.download_all_reporting_exports(
                path=self.local_file_path
            )
            logger.info("Downloaded the data from the Genesys into the CSV.")
            # in order to wait for API GET request call it
            logger.info("Waiting for caching data in Genesys database.")
            time.sleep(20)
            genesys.delete_all_reporting_exports()
            logger.info(f"All existing reports were delted.")

            return file_names

        elif view_type is None and end_point == "conversations/details/query":
            if len(post_data_list) > 1:
                logger.error("Not available more than one body for this end-point.")
                raise signals.FAIL(message="Stopping the flow.")

            stop_loop = False
            page_counter = post_data_list[0]["paging"]["pageNumber"]
            merged_data = {}
            while not stop_loop:
                report = genesys.genesys_generate_exports(
                    post_data_list=post_data_list, end_point=end_point
                )
                merged_data_frame = self.merge_nested_df(report["conversations"])

                merged_data.update(
                    {post_data_list[0]["paging"]["pageNumber"]: merged_data_frame}
                )

                if page_counter == 1:
                    max_calls = int(np.ceil(report["totalHits"] / 100))
                elif page_counter == max_calls:
                    stop_loop = True

                post_data_list[0]["paging"]["pageNumber"] += 1

                if post_data_list[0]["paging"]["pageNumber"] == 3:
                    break

            sys.exit()
