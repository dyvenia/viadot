"""Genesys Cloud API connector."""

import asyncio
import base64
from io import StringIO
import json
import time
from typing import Any

import aiohttp
from aiolimiter import AsyncLimiter
import numpy as np
import pandas as pd
from pydantic import BaseModel

from viadot.config import get_source_credentials
from viadot.exceptions import APIError
from viadot.sources.base import Source
from viadot.utils import add_viadot_metadata_columns, handle_api_response, validate


class GenesysCredentials(BaseModel):
    """Validate Genesys credentials.

    Two key values are held in the Genesys connector:
        - client_id: The unique ID for the organization.
        - client_secret: Secret string of characters to have access to divisions.

    Args:
        BaseModel (pydantic.main.ModelMetaclass): A base class for creating Pydantic
            models.
    """

    client_id: str
    client_secret: str


class Genesys(Source):
    ENVIRONMENTS = (
        "cac1.pure.cloud",
        "sae1.pure.cloud",
        "mypurecloud.com",
        "usw2.pure.cloud",
        "aps1.pure.cloud",
        "apne3.pure.cloud",
        "apne2.pure.cloud",
        "mypurecloud.com.au",
        "mypurecloud.jp",
        "mypurecloud.ie",
        "mypurecloud.de",
        "euw2.pure.cloud",
        "euc2.pure.cloud",
        "mec1.pure.cloud",
    )

    def __init__(
        self,
        *args,
        credentials: GenesysCredentials | None = None,
        config_key: str = "genesys",
        verbose: bool = False,
        environment: str = "mypurecloud.de",
        **kwargs,
    ):
        """Genesys Cloud API connector.

        Provides functionalities for connecting to Genesys Cloud API and downloading
        generated reports. It includes the following features:

        - Generate reports inside Genesys.
        - Download the reports previously created.
        - Direct connection to Genesys Cloud API, via GET method, to retrieve the data
          without any report creation.
        - Remove any report previously created.

        Args:
            credentials (Optional[GenesysCredentials], optional): Genesys credentials.
                Defaults to None
            config_key (str, optional): The key in the viadot config holding relevant
                credentials. Defaults to "genesys".
            verbose (bool, optional): Increase the details of the logs printed on the
                screen. Defaults to False.
            environment (str, optional): the domain that appears for Genesys Cloud
                Environment based on the location of your Genesys Cloud organization.
                Defaults to "mypurecloud.de".

        Examples:
            genesys = Genesys(
                credentials=credentials,
                config_key=config_key,
                verbose=verbose,
                environment=environment,
            )
            genesys.api_connection(
                endpoint=endpoint,
                queues_ids=queues_ids,
                view_type=view_type,
                view_type_time_sleep=view_type_time_sleep,
                post_data_list=post_data_list,
                normalization_sep=normalization_sep,
            )
            data_frame = genesys.to_df(
                drop_duplicates=drop_duplicates,
                validate_df_dict=validate_df_dict,
            )

        Raises:
            CredentialError: If credentials are not provided in local_config or directly
                as a parameter.
            APIError: When the environment variable is not among the available.
        """
        raw_creds = credentials or get_source_credentials(config_key)
        validated_creds = dict(GenesysCredentials(**raw_creds))

        super().__init__(*args, credentials=validated_creds, **kwargs)

        self.verbose = verbose
        self.data_returned = {}
        self.new_report = "{}"  # ???

        if environment in self.ENVIRONMENTS:
            self.environment = environment
        else:
            raise APIError(
                f"Environment '{environment}' not available"
                + " in Genesys Cloud Environments."
            )

    @property
    def headers(self) -> dict[str, Any]:
        """Get request headers.

        Returns:
            Dict[str, Any]: Request headers with token.
        """
        client_id = self.credentials.get("client_id", "")
        client_secret = self.credentials.get("client_secret", "")
        authorization = base64.b64encode(
            bytes(client_id + ":" + client_secret, "ISO-8859-1")
        ).decode("ascii")
        request_headers = {
            "Authorization": f"Basic {authorization}",
            "Content-Type": "application/x-www-form-urlencoded",
        }
        request_body = {"grant_type": "client_credentials"}
        response = handle_api_response(
            f"https://login.{self.environment}/oauth/token",
            data=request_body,
            headers=request_headers,
            method="POST",
            timeout=3600,
        )

        if response.ok:
            self.logger.info("Temporary authorization token was generated.")
        else:
            self.logger.info(
                f"Failure: { response.status_code !s} - { response.reason }"
            )
        response_json = response.json()

        return {
            "Authorization": f"{ response_json['token_type'] }"
            + f" { response_json['access_token']}",
            "Content-Type": "application/json",
        }

    def _api_call(
        self,
        endpoint: str,
        post_data_list: list[str],
        method: str,
        params: dict[str, Any] | None = None,
        time_between_api_call: float = 0.5,
    ) -> dict[str, Any]:
        """General method to connect to Genesys Cloud API and generate the response.

        Args:
            endpoint (str): Final end point to the API.
            post_data_list (List[str]): List of string templates to generate json body.
            method (str): Type of connection to the API. Defaults to "POST".
            params (Optional[Dict[str, Any]], optional): Parameters to be passed into
                the POST call. Defaults to None.
            time_between_api_call (int, optional): The time, in seconds, to sleep the
                call to the API. Defaults to 0.5.

        Raises:
            RuntimeError: There is no current event loop in asyncio thread.

        Returns:
            Dict[str, Any]: Genesys Cloud API response. When the endpoint requires to
                create a report within Genesys Cloud, the response is just useless
                information. The useful data must be downloaded from apps.{environment}
                through another requests.
        """
        limiter = AsyncLimiter(2, 15)
        semaphore = asyncio.Semaphore(value=1)
        url = f"https://api.{self.environment}/api/v2/{endpoint}"

        async def generate_post():
            for data_to_post in post_data_list:
                payload = json.dumps(data_to_post)

                async with aiohttp.ClientSession() as session:
                    await semaphore.acquire()

                    async with limiter:
                        if method == "POST":
                            async with session.post(
                                url,
                                headers=self.headers,
                                data=payload,
                            ) as resp:
                                # global new_report
                                self.new_report = await resp.read()
                                message = "Generated report export ---"
                                if self.verbose:
                                    message += f"\n {payload}."
                                    self.logger.info(message)

                                semaphore.release()

                        elif method == "GET":
                            async with session.get(
                                url,
                                headers=self.headers,
                                params=params,
                            ) as resp:
                                self.new_report = await resp.read()
                                message = "Connecting to Genesys Cloud"
                                if self.verbose:
                                    message += f": {params}."
                                    self.logger.info(message)

                                semaphore.release()

                await asyncio.sleep(time_between_api_call)

        try:
            loop = asyncio.get_event_loop()
        except RuntimeError as err:
            if str(err).startswith("There is no current event loop in thread"):
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
            else:
                raise
        coroutine = generate_post()
        loop.run_until_complete(coroutine)

        return json.loads(self.new_report.decode("utf-8"))

    def _load_reporting_exports(
        self,
        page_size: int = 100,
    ) -> dict[str, Any]:
        """Consult the status of the reports created in Genesys Cloud.

        Args:
            page_size (int, optional): The number of items on page to print.
                Defaults to 100.
            verbose (bool, optional): Switch on/off for logging messages.
                Defaults to False.

        Raises:
            APIError: Failed to loaded the exports from Genesys Cloud.

        Returns:
            Dict[str, Any]: Schedule genesys report.
        """
        response = handle_api_response(
            url=f"https://api.{self.environment}/api/v2/"
            + f"analytics/reporting/exports?pageSize={page_size}",
            headers=self.headers,
            method="GET",
        )

        response_ok = 200
        if response.status_code == response_ok:
            return response.json()

        self.logger.error(f"Failed to loaded all exports. - {response.content}")
        msg = "Failed to loaded all exports."
        raise APIError(msg)

    def _get_reporting_exports_url(self, entities: list[str]) -> tuple[list[str]]:
        """Collect all reports created in Genesys Cloud.

        Args:
            entities (List[str]): List of dictionaries with all the reports information
                available in Genesys Cloud.

        Returns:
            Tuple[List[str]]: A tuple with Lists of IDs and URLs.
        """
        ids = []
        urls = []
        status = []
        for entity in entities:
            ids.append(entity.get("id"))
            urls.append(entity.get("downloadUrl"))
            status.append(entity.get("status"))

        if "FAILED" in status:
            self.logger.error("Some reports have not been successfully created.")
        if "RUNNING" in status:
            self.logger.warning(
                "Some reports are still being created and can not be downloaded."
            )
        if self.verbose:
            message = "".join(
                [f"\t{i} -> {j} \n" for i, j in zip(ids, status, strict=False)]
            )
            self.logger.info(f"Report status:\n{message}")

        return ids, urls

    def _delete_report(self, report_id: str) -> None:
        """Delete a particular report in Genesys Cloud.

        Args:
            report_id (str): Id of the report to be deleted.
        """
        delete_response = handle_api_response(
            url=f"https://api.{self.environment}/api/v2/"
            + f"analytics/reporting/exports/{report_id}",
            headers=self.headers,
            method="DELETE",
        )
        # Ok-ish responses (includes eg. 204 No Content)
        ok_response_limit = 300
        if delete_response.status_code < ok_response_limit:
            self.logger.info(
                f"Successfully deleted report '{report_id}' from Genesys API."
            )
        else:
            self.logger.error(
                f"Failed to delete report '{report_id}' "
                + f"from Genesys API. - {delete_response.content}"
            )

    def _download_report(
        self,
        report_url: str,
        drop_duplicates: bool = True,
    ) -> pd.DataFrame:
        """Download report from Genesys Cloud.

        Args:
            report_url (str): url to report, fetched from json response.
            drop_duplicates (bool, optional): Decide if drop duplicates.
                Defaults to True.

        Returns:
            pd.DataFrame: Data in a pandas DataFrame.
        """
        response = handle_api_response(url=f"{report_url}", headers=self.headers)

        # Ok-ish responses (includes eg. 204 No Content)
        ok_response_limit = 300
        if response.status_code < ok_response_limit:
            self.logger.info(
                f"Successfully downloaded report from Genesys API ('{report_url}')."
            )

        else:
            msg = (
                "Failed to download report from"
                + f" Genesys API ('{report_url}'). - {response.content}"
            )
            self.logger.error(msg)

        dataframe = pd.read_csv(StringIO(response.content.decode("utf-8")))

        if drop_duplicates is True:
            dataframe.drop_duplicates(inplace=True, ignore_index=True)

        return dataframe

    def _merge_conversations(self, data_to_merge: list) -> pd.DataFrame:  # noqa: C901, PLR0912
        """Merge all the conversations data into a single data frame.

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
        # Columns that will be the reference for the next LEVEL
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
            except KeyError as err:
                self.logger.info(f"Key {err} not appearing in the response.")

        # LEVEL 3
        conversations_df = {}
        for i, conversation in enumerate(data_to_merge):
            # Not all "sessions" have the same data, and that creates
            #   problems of standardization
            # Empty data will be added to columns where there is not to avoid
            #   future errors.
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

        # MERGING ALL LEVELS
        # LEVELS 3
        for i_3, key in enumerate(list(conversations_df.keys())):
            if i_3 == 0:
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
        return pd.merge(df0, dff1, how="outer", on=["conversationId"])

    # This is way too complicated for what it's doing...
    def api_connection(  # noqa: PLR0912, PLR0915, C901.
        self,
        endpoint: str | None = None,
        queues_ids: list[str] | None = None,
        view_type: str | None = None,
        view_type_time_sleep: int = 10,
        post_data_list: list[dict[str, Any]] | None = None,
        time_between_api_call: float = 0.5,
        normalization_sep: str = ".",
    ) -> None:
        """General method to connect to Genesys Cloud API and generate the response.

        Args:
            endpoint (Optional[str], optional): Final end point to the API.
                Defaults to None.

                Custom endpoints have specific key words, and parameters:
                Example:
                    - "routing/queues/{id}/members": "routing_queues_members"
                    - members_ids = ["xxxxxxxxx", "xxxxxxxxx", ...]
            queues_ids (Optional[List[str]], optional): List of queues ids to consult
                the members. Defaults to None.
            view_type (Optional[str], optional): The type of view export job to be
                created. Defaults to None.
            view_type_time_sleep (int, optional): Waiting time to retrieve data from
                Genesys Cloud API. Defaults to 10.
            post_data_list (Optional[List[Dict[str, Any]]], optional): List of string
                templates to generate json body in POST calls to the API.
                Defaults to None.
            time_between_api_call (int, optional): The time, in seconds, to sleep the
                call to the API. Defaults to 0.5.
            normalization_sep (str, optional): Nested records will generate names
                separated by sep. Defaults to ".".

        Raises:
            APIError: Some or No reports were not created.
            APIError: At different endpoints:
                - 'analytics/conversations/details/query': only one body must be used.
                - 'routing_queues_members': extra parameter `queues_ids` must be
                    included.
        """
        self.logger.info(
            f"Connecting to the Genesys Cloud using the endpoint: {endpoint}"
        )

        if endpoint == "analytics/reporting/exports":
            self._api_call(
                endpoint=endpoint,
                post_data_list=post_data_list,
                time_between_api_call=time_between_api_call,
                method="POST",
            )

            msg = (
                f"Waiting {view_type_time_sleep} seconds for"
                + " caching data from Genesys Cloud API."
            )
            self.logger.info(msg)
            time.sleep(view_type_time_sleep)

            request_json = self._load_reporting_exports()
            entities = request_json["entities"]

            if isinstance(entities, list):
                ids, urls = self._get_reporting_exports_url(entities)
                if len(entities) != len(post_data_list):
                    self.logger.warning(
                        f"There are {len(entities)} available reports in Genesys, "
                        f"and where sent {len(post_data_list)} reports. "
                        "Unsed reports will be removed."
                    )
            else:
                APIError(
                    "There are no reports to be downloaded."
                    f"May be {view_type_time_sleep} should be increased."
                )

            # download and delete reports created
            count = 0
            raise_api_error = False
            for qid, url in zip(ids, urls, strict=False):
                if url is not None:
                    df_downloaded = self._download_report(report_url=url)

                    time.sleep(1.0)
                    # remove resume rows
                    if view_type in ["queue_performance_detail_view"]:
                        criteria = (
                            df_downloaded["Queue Id"]
                            .apply(lambda x: str(x).split(";"))
                            .apply(lambda x: not len(x) > 1)
                        )
                        df_downloaded = df_downloaded[criteria]

                    self.data_returned.update({count: df_downloaded})
                else:
                    self.logger.error(
                        f"Report id {qid} didn't have time to be created. "
                        + "Consider increasing the `view_type_time_sleep` parameter "
                        + f">> {view_type_time_sleep} seconds to allow Genesys Cloud "
                        + "to conclude the report creation."
                    )
                    raise_api_error = True

                self._delete_report(qid)

                count += 1  # noqa: SIM113

            if raise_api_error:
                msg = "Some reports creation failed."
                raise APIError(msg)

        elif endpoint == "analytics/conversations/details/query":
            if len(post_data_list) > 1:
                msg = "Not available more than one body for this end-point."
                raise APIError(msg)

            stop_loop = False
            page_counter = post_data_list[0]["paging"]["pageNumber"]
            self.logger.info(
                "Restructuring the response in order to be able to insert it into a "
                + "data frame.\n\tThis task could take a few minutes.\n"
            )
            while not stop_loop:
                report = self._api_call(
                    endpoint=endpoint,
                    post_data_list=post_data_list,
                    time_between_api_call=time_between_api_call,
                    method="POST",
                )

                merged_data_frame = self._merge_conversations(report["conversations"])
                self.data_returned.update(
                    {
                        int(post_data_list[0]["paging"]["pageNumber"])
                        - 1: merged_data_frame
                    }
                )

                if page_counter == 1:
                    max_calls = int(np.ceil(report["totalHits"] / 100))
                if page_counter == max_calls:
                    stop_loop = True

                post_data_list[0]["paging"]["pageNumber"] += 1
                page_counter += 1

        elif endpoint in ["routing/queues", "users"]:
            page = 1
            self.logger.info(
                "Restructuring the response in order to be able to insert it into a "
                + "data frame.\n\tThis task could take a few minutes.\n"
            )
            while True:
                if endpoint == "routing/queues":
                    params = {"pageSize": 500, "pageNumber": page}
                elif endpoint == "users":
                    params = {
                        "pageSize": 500,
                        "pageNumber": page,
                        "expand": "presence,dateLastLogin,groups"
                        + ",employerInfo,lasttokenissued",
                        "state": "any",
                    }
                response = self._api_call(
                    endpoint=endpoint,
                    post_data_list=post_data_list,
                    time_between_api_call=time_between_api_call,
                    method="GET",
                    params=params,
                )

                if response["entities"]:
                    df_response = pd.json_normalize(
                        response["entities"],
                        sep=normalization_sep,
                    )
                    self.data_returned.update({page - 1: df_response})

                    page += 1
                else:
                    break

        elif endpoint == "routing_queues_members":
            counter = 0
            if queues_ids is None:
                self.logger.error(
                    "This endpoint requires `queues_ids` parameter to work."
                )
                APIError("This endpoint requires `queues_ids` parameter to work.")

            for qid in queues_ids:
                self.logger.info(f"Downloading Agents information from Queue: {qid}")
                page = 1
                while True:
                    response = self._api_call(
                        endpoint=f"routing/queues/{qid}/members",
                        params={"pageSize": 100, "pageNumber": page},
                        post_data_list=post_data_list,
                        time_between_api_call=time_between_api_call,
                        method="GET",
                    )

                    if response["entities"]:
                        df_response = pd.json_normalize(response["entities"])
                        # drop personal information
                        columns_to_drop = {
                            "user.addresses",
                            "user.primaryContactInfo",
                            "user.images",
                        }.intersection(df_response.columns)
                        df_response.drop(
                            columns_to_drop,
                            axis=1,
                            inplace=True,
                        )
                        self.data_returned.update({counter: df_response})

                        page += 1
                        counter += 1
                    else:
                        break

    @add_viadot_metadata_columns
    def to_df(
        self,
        if_empty: str = "warn",
        **kwargs,
    ) -> pd.DataFrame:
        """Generate a pandas DataFrame from self.data_returned.

        Args:
            drop_duplicates (bool, optional): Remove duplicates from the DataFrame.
                Defaults to False.
            validate_df_dict (Optional[Dict[str, Any]], optional): A dictionary with
                optional list of tests to verify the output dataframe. Defaults to None.

        Returns:
            pd.Dataframe: The response data as a pandas DataFrame plus viadot metadata.
        """
        drop_duplicates = kwargs.get("drop_duplicates", False)
        validate_df_dict = kwargs.get("validate_df_dict", None)
        super().to_df(if_empty=if_empty)

        for key in list(self.data_returned.keys()):
            if key == 0:
                data_frame = self.data_returned[key]
            else:
                data_frame = pd.concat([data_frame, self.data_returned[key]])

        if drop_duplicates:
            data_frame.drop_duplicates(inplace=True)

        if validate_df_dict:
            validate(df=data_frame, tests=validate_df_dict)

        if len(self.data_returned) == 0:
            data_frame = pd.DataFrame()
            self._handle_if_empty(
                if_empty=if_empty,
                message="The response does not contain any data.",
            )
        else:
            data_frame.reset_index(inplace=True, drop=True)

        return data_frame
