import os
import time
from typing import Any, Dict, List

import numpy as np
import pandas as pd
import prefect
from pandas import DataFrame
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
        report_url: str = None,
        report_columns: List[str] = None,
        local_file_path: str = "",
        sep: str = "\t",
        conversationId_list: List[str] = None,
        key_list: List[str] = None,
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
            report_url (str, optional): The url of report generated in json response. Defaults to None.
            report_columns (List[str], optional): List of exisiting column in report. Defaults to None.
            local_file_path (str, optional): The local path from which to upload the file(s). Defaults to "".
            sep (str, optional): Separator in csv file. Defaults to "\t".
            conversationId_list (List[str], optional): List of conversationId passed as attribute of GET method. Defaults to None.
            key_list (List[str], optional): List of keys needed to specify the columns in the GET request method. Defaults to None.
            timeout(int, optional): The amount of time (in seconds) to wait while running this task before
                a timeout occurs. Defaults to 3600.
        """

        self.logger = prefect.context.get("logger")
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
        self.sep = sep
        self.conversationId_list = conversationId_list
        self.key_list = key_list

        super().__init__(
            name=self.report_name,
            timeout=timeout,
            *args,
            **kwargs,
        )

    def __call__(self, *args, **kwargs):
        """Download Genesys data to CSV"""
        return super().__call__(*args, **kwargs)

    def merge_conversations_dfs(self, data_to_merge: list) -> DataFrame:
        """Method to merge all the conversations data into a single data frame.

        Args:
            data_to_merge (list): List with all the conversations in json format.
            Example for all levels data to merge:
                {
                "conversations": [
                    {
                        **** LEVEL 0 data ****
                        "participants": [
                            {
                                **** LEVEL 1 data ****
                                "sessions": [
                                    {
                                        "agentBullseyeRing": 1,
                                        **** LEVEL 2 data ****
                                        "mediaEndpointStats": [
                                            {
                                                **** LEVEL 3 data ****
                                            },
                                        ],
                                        "metrics": [
                                            {
                                                **** LEVEL 3 data ****
                                            },
                                        ],
                                        "segments": [
                                            {
                                                **** LEVEL 3 data ****
                                            },
                                            {
                                                **** LEVEL 3 data ****
                                            },
                                        ],
                                    }
                                ],
                            },
                            {
                                "participantId": "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx",
                                **** LEVEL 1 data ****
                                "sessions": [
                                    {
                                        **** LEVEL 2 data ****
                                        "mediaEndpointStats": [
                                            {
                                                **** LEVEL 3 data ****
                                            }
                                        ],
                                        "flow": {
                                            **** LEVEL 2 data ****
                                        },
                                        "metrics": [
                                            {
                                                **** LEVEL 3 data ****
                                            },
                                        ],
                                        "segments": [
                                            {
                                                **** LEVEL 3 data ****
                                            },
                                        ],
                                    }
                                ],
                            },
                        ],
                    }
                ],
                "totalHits": 100,
            }

        Returns:
            DataFrame: A single data frame with all the content.
        """
        # LEVEL 0
        df0 = pd.json_normalize(data_to_merge)
        df0.drop(["participants"], axis=1, inplace=True)

        # LEVEL 1
        df1 = pd.json_normalize(
            data_to_merge,
            record_path=["participants"],
            meta=["conversationId"],
        )
        df1.drop(["sessions"], axis=1, inplace=True)

        # LEVEL 2
        df2 = pd.json_normalize(
            data_to_merge,
            record_path=["participants", "sessions"],
            meta=[
                ["participants", "externalContactId"],
                ["participants", "participantId"],
            ],
            errors="ignore",
            sep="_",
        )
        df2.rename(
            columns={
                "participants_externalContactId": "externalContactId",
                "participants_participantId": "participantId",
            },
            inplace=True,
        )
        for key in ["metrics", "segments", "mediaEndpointStats"]:
            try:
                df2.drop([key], axis=1, inplace=True)
            except KeyError as e:
                logger.info(f"Key {e} not appearing in the response.")

        # LEVEL 3
        # not all levels 3 have the same data, and that creates problems of standardization
        # so I add empty data where it is not available to avoid future errors
        conversations_df = {}
        for i, conversation in enumerate(data_to_merge):
            for j, entry_0 in enumerate(conversation["participants"]):
                for key in list(entry_0.keys()):
                    if key == "sessions":
                        for k, entry_1 in enumerate(entry_0[key]):
                            if "metrics" not in list(entry_1.keys()):
                                conversation["participants"][j][key][k]["metrics"] = []
                            if "segments" not in list(entry_1.keys()):
                                conversation["participants"][j][key][k]["segments"] = []
                            if "mediaEndpointStats" not in list(entry_1.keys()):
                                conversation["participants"][j][key][k][
                                    "mediaEndpointStats"
                                ] = []
            # LEVEL 3 metrics
            df3_1 = pd.json_normalize(
                conversation,
                record_path=["participants", "sessions", "metrics"],
                meta=[
                    ["participants", "sessions", "sessionId"],
                ],
                errors="ignore",
                record_prefix="metrics_",
                sep="_",
            )
            df3_1.rename(
                columns={"participants_sessions_sessionId": "sessionId"}, inplace=True
            )
            # LEVEL 3 segments
            df3_2 = pd.json_normalize(
                conversation,
                record_path=["participants", "sessions", "segments"],
                meta=[
                    ["participants", "sessions", "sessionId"],
                ],
                errors="ignore",
                record_prefix="segments_",
                sep="_",
            )
            df3_2.rename(
                columns={"participants_sessions_sessionId": "sessionId"}, inplace=True
            )
            # LEVEL 3 mediaEndpointStats
            df3_3 = pd.json_normalize(
                conversation,
                record_path=["participants", "sessions", "mediaEndpointStats"],
                meta=[
                    ["participants", "sessions", "sessionId"],
                ],
                errors="ignore",
                record_prefix="mediaEndpointStats_",
                sep="_",
            )
            df3_3.rename(
                columns={"participants_sessions_sessionId": "sessionId"}, inplace=True
            )

            # merging all LEVELs 3 from the same conversation
            dff3_tmp = pd.concat([df3_1, df3_2])
            dff3 = pd.concat([dff3_tmp, df3_3])

            conversations_df.update({i: dff3})

        # NERGING ALL LEVELS
        # LEVELS 3
        for l, key in enumerate(list(conversations_df.keys())):
            if l == 0:
                dff3_f = conversations_df[key]
            else:
                dff3_f = pd.concat([dff3_f, conversations_df[key]])

        # LEVEL 3 with LEVEL 2
        dff2 = pd.merge(dff3_f, df2, how="outer", on=["sessionId"])

        # LEVEL 2 with LEVEL 1
        dff1 = pd.merge(
            df1, dff2, how="outer", on=["externalContactId", "participantId"]
        )

        # LEVEL 1 with LEVEL 0
        dff = pd.merge(df0, dff1, how="outer", on=["conversationId"])

        return dff

    @defaults_from_attrs(
        "report_name",
        "view_type",
        "environment",
        "report_url",
        "post_data_list",
        "start_date",
        "end_date",
        "report_columns",
        "credentials_genesys",
        "conversationId_list",
        "key_list",
    )
    def run(
        self,
        report_name: str = None,
        view_type: str = None,
        view_type_time_sleep: int = 80,
        environment: str = None,
        report_url: str = None,
        post_data_list: List[str] = None,
        end_point: str = "analytics/reporting/exports",
        start_date: str = None,
        end_date: str = None,
        report_columns: List[str] = None,
        conversationId_list: List[str] = None,
        key_list: List[str] = None,
        credentials_genesys: Dict[str, Any] = None,
    ) -> List[str]:
        """
        Task for downloading data from the Genesys API to DF.

        Args:
            report_name (str, optional): The name of this task. Defaults to a general name 'genesys_to_csv'.
            view_type (str, optional): The type of view export job to be created. Defaults to None.
            view_type_time_sleep (int, optional): Waiting time to retrieve data from Genesys API. Defaults to 80.
            post_data_list (List[str], optional): List of string templates to generate json body. Defaults to None.
            end_point (str, optional): Final end point for Genesys connection. Defaults to "analytics/reporting/exports".
            credentials_genesys (Dict[str, Any], optional): Credentials to connect with Genesys API containing CLIENT_ID. Defaults to None.
            start_date (str, optional): Start date of the report. Defaults to None.
            end_date (str, optional): End date of the report. Defaults to None.
            environment (str, optional): Adress of host server. Defaults to None than will be used enviroment from credentials.
            report_url (str, optional): The url of report generated in json response. Defaults to None.
            report_columns (List[str], optional): List of exisiting column in report. Defaults to None.
            conversationId_list (List[str], optional): List of conversationId passed as attribute of GET method. Defaults to None.
            key_list (List[str], optional): List of keys needed to specify the columns in the GET request method. Defaults to None.

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
            report_url=report_url,
            report_columns=report_columns,
        )

        if view_type == "queue_performance_detail_view":
            genesys.genesys_api_connection(
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
            genesys.genesys_api_connection(
                post_data_list=post_data_list, end_point=end_point
            )
            logger.info(
                f"Waiting for getting data in Genesys database ({view_type_time_sleep} seconds)."
            )
            time.sleep(view_type_time_sleep)

            genesys.get_reporting_exports_data()

        if view_type is not None and end_point == "analytics/reporting/exports":
            failed = [col for col in np.array(genesys.report_data).T][-1]

            if "FAILED" in failed and "COMPLETED" in failed:
                logger.warning("Some reports failed.")

            if len(genesys.report_data) == 0 or np.unique(failed)[0] == "FAILED":
                genesys.delete_all_reporting_exports()
                logger.warning(f"All existing reports were deleted.")
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
            logger.info(f"All existing reports were deleted.")

            return file_names

        elif view_type is None and end_point == "analytics/conversations/details/query":
            if len(post_data_list) > 1:
                logger.error("Not available more than one body for this end-point.")
                raise signals.FAIL(message="Stopping the flow.")

            stop_loop = False
            page_counter = post_data_list[0]["paging"]["pageNumber"]
            merged_data = {}
            while not stop_loop:
                report = genesys.genesys_api_connection(
                    post_data_list=post_data_list, end_point=end_point
                )
                merged_data_frame = self.merge_conversations_dfs(
                    report["conversations"]
                )

                merged_data.update(
                    {post_data_list[0]["paging"]["pageNumber"]: merged_data_frame}
                )

                if page_counter == 1:
                    max_calls = int(np.ceil(report["totalHits"] / 100))
                if page_counter == max_calls:
                    stop_loop = True

                post_data_list[0]["paging"]["pageNumber"] += 1
                page_counter += 1

            for i, key in enumerate(list(merged_data.keys())):
                if i == 0:
                    final_df = merged_data[key]
                else:
                    final_df = pd.concat([final_df, merged_data[key]])

            date = start_date.replace("-", "")
            file_name = f"conversations_detail_{date}".upper() + ".csv"

            final_df.to_csv(
                os.path.join(self.local_file_path, file_name),
                index=False,
                sep="\t",
            )

            logger.info("Downloaded the data from the Genesys into the CSV.")

            return [file_name]

        elif view_type is None and end_point == "conversations":
            data_list = []

            for id in conversationId_list:
                json_file = genesys.genesys_api_connection(
                    post_data_list=post_data_list,
                    end_point=f"{end_point}/{id}",
                    method="GET",
                )
                logger.info(f"Generated webmsg_response for {id}")

                attributes = json_file["participants"][0]["attributes"]
                temp_dict = {
                    key: value for (key, value) in attributes.items() if key in key_list
                }
                temp_dict["conversationId"] = json_file["id"]
                data_list.append(temp_dict)

            df = pd.DataFrame(data_list)
            df = df[df.columns[-1:]].join(df[df.columns[:-1]])

            start = start_date.replace("-", "")
            end = end_date.replace("-", "")

            file_name = f"WEBMESSAGE_{start}-{end}.csv"
            df.to_csv(
                os.path.join(file_name),
                index=False,
                sep="\t",
            )

            logger.info("Downloaded the data from the Genesys into the CSV.")

            return [file_name]
