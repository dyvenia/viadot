import asyncio
import base64
import json
import logging
import time
from io import StringIO
from typing import Any, Dict, List, Optional, Tuple

import aiohttp
import pandas as pd
from aiolimiter import AsyncLimiter
from pydantic import BaseModel

from viadot.config import get_source_credentials
from viadot.exceptions import APIError, CredentialError
from viadot.sources.base import Source
from viadot.utils import add_viadot_metadata_columns, handle_api_response

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# add logger to console
# console_handler = logging.StreamHandler()
# console_handler.setLevel(logging.INFO)
# logger.addHandler(console_handler)


class GENESYS_CREDENTIALS(BaseModel):
    """Checking for values in Genesys credentials dictionary.
    Two key values are held in the Genesys connector:
        - client_id: The unique ID for the organization.
        - client_secret: Secret string of characters to have access to divisions.
    Args:
        BaseModel (pydantic.main.ModelMetaclass): A base class for creating Pydantic models.
    """

    client_id: str
    client_secret: str


class Genesys(Source):
    """
    Class implementing the Genesys API.

    Documentation for this API is available at: https://developer.genesys.cloud/devapps/api-explorer.
    """

    ENVIRONMETNS = [
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
    ]

    def __init__(
        self,
        credentials: Optional[GENESYS_CREDENTIALS] = None,
        config_key: str = "genesys",
        verbose: bool = False,
        environment: str = "mypurecloud.de",
        *args,
        **kwargs,
    ):
        """
        Description:
            Creation of a Genesys instance.

        Args:
            credentials (Optional[GENESYS_CREDENTIALS], optional): Genesys credentials.
                Defaults to None
            config_key (str, optional): The key in the viadot config holding relevant credentials.
                Defaults to "genesys".
            verbose (bool, optional): Increase the details of the logs printed on the screen.
                Defaults to False.
            environment (str, optional): the domain that appears for Genesys Cloud Environment
                based on the location of your Genesys Cloud organization. Defaults to "mypurecloud.de".

        Raises:
            CredentialError: If credentials are not provided in local_config or directly as a parameter.
            APIError: When the environment variable is not among the available.
        """

        credentials = credentials or get_source_credentials(config_key) or None
        if credentials is None:
            raise CredentialError("Missing credentials.")
        self.credentials = credentials
        super().__init__(*args, credentials=self.credentials, **kwargs)

        self.verbose = verbose

        if environment in self.ENVIRONMETNS:
            self.environment = environment
        else:
            raise APIError(
                f"Environment '{environment}' not available in Genesys Cloud Environments."
            )

    @property
    def authorization_token(self, verbose: bool = False) -> Dict[str, Any]:
        """
        Get authorization token with request headers.

        Args:
            verbose (bool, optional): Switch on/off for logging messages. Defaults to False.

        Returns:
            Dict[str, Any]: Request headers with token.
        """
        CLIENT_ID = self.credentials.get("client_id", "")
        CLIENT_SECRET = self.credentials.get("client_secret", "")
        authorization = base64.b64encode(
            bytes(CLIENT_ID + ":" + CLIENT_SECRET, "ISO-8859-1")
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
        if verbose:
            if response.status_code == 200:
                logger.info("Temporary authorization token was generated.")
            else:
                logger.info(
                    f"Failure: { str(response.status_code) } - { response.reason }"
                )
        response_json = response.json()
        request_headers = {
            "Authorization": f"{ response_json['token_type'] } { response_json['access_token']}",
            "Content-Type": "application/json",
        }

        return request_headers

    def _api_call(
        self,
        endpoint: str,
        post_data_list: List[str],
        method: str,
        params: Optional[Dict[str, Any]] = None,
        sleep_time: int = 0.5,
    ) -> Dict[str, Any]:
        """
        Description:
            General method to connect to Genesys Cloud API and generate the response.

        Args:
            endpoint (str): Final end point to the API.
            post_data_list (List[str]): List of string templates to generate json body.
            method (str): Type of connection to the API. Defaults to "POST".
            params (Optional[Dict[str, Any]], optional): Parameters to be passed into the POST call.
                Defaults to None.
            sleep_time (int, optional): The time, in seconds, to sleep the call to the API.
                Defaults to 0.5

        Returns:
            Dict[str, Any]: Genesys Cloud API response. When the endpoint requires to create
                a report within Genesys Cloud, the response is just useless information. The
                useful data must be downloaded from apps.{environment} through another requests.
                First one to get the 'download' endpoint, and finally,
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
                                headers=self.authorization_token,
                                data=payload,
                            ) as resp:
                                global new_report
                                new_report = await resp.read()
                                message = "Generated report export ---"
                                if self.verbose:
                                    message += f"\n {payload}."
                                logger.info(message)

                                semaphore.release()

                        elif method == "GET":
                            async with session.get(
                                url,
                                headers=self.authorization_token,
                                params=params,
                            ) as resp:
                                new_report = await resp.read()
                                semaphore.release()

                await asyncio.sleep(sleep_time)

        try:
            loop = asyncio.get_event_loop()
        except RuntimeError as e:
            if str(e).startswith("There is no current event loop in thread"):
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
            else:
                raise e
        coroutine = generate_post()
        loop.run_until_complete(coroutine)

        return json.loads(new_report.decode("utf-8"))

    def _load_reporting_exports(
        self,
        page_size: int = 100,
    ) -> Dict[str, Any]:
        """
        Description:
            Consult the status of the reports created in Genesys Cloud.

        Args:
            page_size (int, optional): The number of items on page to print. Defaults to 100.
            verbose (bool, optional): Switch on/off for logging messages. Defaults to False.

        Returns:
            Dict[str, Any]: Schedule genesys report.
        """
        response = handle_api_response(
            url=f"https://api.{self.environment}/api/v2/analytics/reporting/exports?pageSize={page_size}",
            headers=self.authorization_token,
            method="GET",
        )

        if response.status_code == 200:
            return response.json()
        else:
            logger.error(f"Failed to loaded all exports. - {response.content}")
            raise APIError("Failed to loaded all exports.")

    def _get_reporting_exports_url(self, entities: List[str]) -> Tuple[List[str]]:
        """
        Description:
            Collect all reports created in Genesys Cloud.

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
            # entity.get("filter").get("queueIds", [-1])[0],
            # entity.get("filter").get("mediaTypes", [-1])[0],
            # entity.get("viewType"),
            # entity.get("interval"),
            status.append(entity.get("status"))

        if "FAILED" in status:
            logger.warning("Some reports have not been successfully created.")
        if "RUNNING" in status:
            logger.warning(
                "Some reports are still being created and can not be downloaded."
            )
        if self.verbose:
            message = "".join([f"\t{i} -> {j} \n" for i, j in zip(ids, status)])
            logger.info(f"Report status:\n {message}")

        return ids, urls

    def _delete_report(self, report_id: str) -> None:
        """
        Description:
            Delete a particular report in Genesys Cloud.

        Args:
            report_id (str): Id of the report to be deleted.
        """
        delete_response = handle_api_response(
            url=f"https://api.{self.environment}/api/v2/analytics/reporting/exports/{report_id}",
            headers=self.authorization_token,
            method="DELETE",
        )
        if delete_response.status_code < 300:
            logger.info(f"Successfully deleted report '{report_id}' from Genesys API.")

        else:
            logger.error(
                f"Failed to deleted report '{report_id}' from Genesys API. - {delete_response.content}"
            )

    def _download_report(
        self,
        report_url: str,
        drop_duplicates: bool = True,
    ) -> pd.DataFrame:
        """
        Description:
            Download report from Genesys Cloud.

        Args:
            report_url (str): url to report, fetched from json response.
            drop_duplicates (bool, optional): Decide if drop duplicates. Defaults to True.

        Returns:
            pd.DataFrame: Data in a pandas Data Frame.
        """
        donwload_response = handle_api_response(
            url=f"{report_url}", headers=self.authorization_token
        )

        if donwload_response.status_code < 300:
            logger.info(
                f"Successfully downloaded report from Genesys API ('{report_url}')."
            )

        else:
            logger.error(
                f"Failed to download report from Genesys API ('{report_url}'). - {donwload_response.content}"
            )

        df = pd.read_csv(StringIO(donwload_response.content.decode("utf-8")))

        if drop_duplicates is True:
            df.drop_duplicates(inplace=True, ignore_index=True)

        return df

    def api_connection(
        self,
        endpoint: Optional[str] = None,
        view_type: Optional[str] = None,
        view_type_time_sleep: int = 10,
        post_data_list: Optional[List[Dict[str, Any]]] = None,
    ) -> None:
        """
        Description:
            General method to connect to Genesys Cloud API and generate the response.

        Args:
            endpoint (Optional[str], optional): Final end point to the API.
                Defaults to None.
            view_type (Optional[str], optional): The type of view export job to be created.
                Defaults to None.
            view_type_time_sleep (int, optional): Waiting time to retrieve data from Genesys
                Cloud API. Defaults to 10.
            post_data_list (Optional[List[Dict[str, Any]]], optional): List of string templates to
                generate json body in POST calls to the API. Defaults to None.
        """

        if (
            endpoint == "analytics/reporting/exports"
            and view_type == "queue_performance_detail_view"
        ):
            self._api_call(
                endpoint=endpoint,
                post_data_list=post_data_list,
                method="POST",
            )
            logger.info(
                f"Waiting {view_type_time_sleep} seconds for caching data from Genesys Cloud API."
            )
            time.sleep(view_type_time_sleep)

            request_json = self._load_reporting_exports()
            entities = request_json["entities"]
            if isinstance(entities, list) and len(entities) == len(post_data_list):
                ids, urls = self._get_reporting_exports_url(entities)
            else:
                logger.error("There are no reports to be downloaded.")

            # download and delete reports created
            count = 0
            self.data_returned = {}
            for id, url in zip(ids, urls):
                df_downloaded = self._download_report(report_url=url)

                time.sleep(1.0)
                # remove resume rows
                criteria = (
                    df_downloaded["Queue Id"]
                    .apply(lambda x: str(x).split(";"))
                    .apply(lambda x: False if len(x) > 1 else True)
                )
                df_downloaded = df_downloaded[criteria]

                self._delete_report(id)

                self.data_returned.update({count: df_downloaded})

                count += 1

    @add_viadot_metadata_columns
    def to_df(self) -> pd.DataFrame:
        """
        Description:
            Generate a Pandas Data Frame with the data in the Response object, and metadata.

        Returns:
            pd.Dataframe: The response data as a Pandas Data Frame plus viadot metadata.
        """

        for key in self.data_returned.keys():
            if key == 0:
                data_frame = self.data_returned[key]
            else:
                data_frame = pd.concat([data_frame, self.data_returned[key]])

        if data_frame.empty:
            self._handle_if_empty(
                if_empty="warn",
                message="The response does not contain any data.",
            )

        return data_frame
