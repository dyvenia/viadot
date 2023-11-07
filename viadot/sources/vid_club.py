import json
import os
import urllib
from datetime import date, datetime, timedelta
from typing import Any, Dict, List, Literal, Tuple

import pandas as pd
from prefect.utilities import logging

from ..exceptions import CredentialError, ValidationError
from ..utils import handle_api_response
from .base import Source

logger = logging.get_logger()


class VidClub(Source):
    """
    A class implementing the Vid Club API.

    Documentation for this API is located at: https://evps01.envoo.net/vipapi/
    There are 4 endpoints where to get the data.
    """

    def __init__(self, credentials: Dict[str, Any], *args, **kwargs):
        """
        Create an instance of VidClub.

        Args:
            credentials (Dict[str, Any]): Credentials to Vid Club APIs containing token.

        Raises:
            CredentialError: If credentials are not provided as a parameter.
        """
        self.headers = {
            "Authorization": "Bearer " + credentials["token"],
            "Content-Type": "application/json",
        }

        super().__init__(*args, credentials=credentials, **kwargs)

    def build_query(
        self,
        from_date: str,
        to_date: str,
        api_url: str,
        items_per_page: int,
        source: Literal["jobs", "product", "company", "survey"] = None,
        region: Literal["bg", "hu", "hr", "pl", "ro", "si", "all"] = "all",
    ) -> str:
        """
        Builds the query from the inputs.

        Args:
            from_date (str): Start date for the query.
            to_date (str): End date for the query, if empty, will be executed as datetime.today().strftime("%Y-%m-%d").
            api_url (str): Generic part of the URL to Vid Club API.
            items_per_page (int): number of entries per page.
            source (Literal["jobs", "product", "company", "survey"], optional): The endpoint source to be accessed. Defaults to None.
            region (Literal["bg", "hu", "hr", "pl", "ro", "si", "all"], optional): Region filter for the query. Defaults to "all". [July 2023 status: parameter works only for 'all' on API]

        Returns:
            str: Final query with all filters added.

        Raises:
            ValidationError: If any source different than the ones in the list are used.
        """
        if source in ["jobs", "product", "company"]:
            url = f"{api_url}{source}?from={from_date}&to={to_date}&region={region}&limit={items_per_page}"
        elif source == "survey":
            url = f"{api_url}{source}?language=en&type=question"
        else:
            raise ValidationError(
                "Pick one these sources: jobs, product, company, survey"
            )
        return url

    def intervals(
        self, from_date: str, to_date: str, days_interval: int
    ) -> Tuple[List[str], List[str]]:
        """
        Breaks dates range into smaller by provided days interval.

        Args:
            from_date (str): Start date for the query in "%Y-%m-%d" format.
            to_date (str): End date for the query, if empty, will be executed as datetime.today().strftime("%Y-%m-%d").
            days_interval (int): Days specified in date range per api call (test showed that 30-40 is optimal for performance).

        Returns:
            List[str], List[str]: Starts and Ends lists that contains information about date ranges for specific period and time interval.

        Raises:
            ValidationError: If the final date of the query is before the start date.
        """

        if to_date == None:
            to_date = datetime.today().strftime("%Y-%m-%d")

        end_date = datetime.strptime(to_date, "%Y-%m-%d").date()
        start_date = datetime.strptime(from_date, "%Y-%m-%d").date()

        from_date_obj = datetime.strptime(from_date, "%Y-%m-%d")

        to_date_obj = datetime.strptime(to_date, "%Y-%m-%d")
        delta = to_date_obj - from_date_obj

        if delta.days < 0:
            raise ValidationError("to_date cannot be earlier than from_date.")

        interval = timedelta(days=days_interval)
        starts = []
        ends = []

        period_start = start_date
        while period_start < end_date:
            period_end = min(period_start + interval, end_date)
            starts.append(period_start.strftime("%Y-%m-%d"))
            ends.append(period_end.strftime("%Y-%m-%d"))
            period_start = period_end
        if len(starts) == 0 and len(ends) == 0:
            starts.append(from_date)
            ends.append(to_date)
        return starts, ends

    def check_connection(
        self,
        source: Literal["jobs", "product", "company", "survey"] = None,
        from_date: str = "2022-03-22",
        to_date: str = None,
        items_per_page: int = 100,
        region: Literal["bg", "hu", "hr", "pl", "ro", "si", "all"] = "all",
        url: str = None,
    ) -> Tuple[Dict[str, Any], str]:
        """
        Initiate first connection to API to retrieve piece of data with information about type of pagination in API URL.
        This option is added because type of pagination for endpoints is being changed in the future from page number to 'next' id.

        Args:
            source (Literal["jobs", "product", "company", "survey"], optional): The endpoint source to be accessed. Defaults to None.
            from_date (str, optional): Start date for the query, by default is the oldest date in the data 2022-03-22.
            to_date (str, optional): End date for the query. By default None, which will be executed as datetime.today().strftime("%Y-%m-%d") in code.
            items_per_page (int, optional): Number of entries per page. 100 entries by default.
            region (Literal["bg", "hu", "hr", "pl", "ro", "si", "all"], optional): Region filter for the query. Defaults to "all". [July 2023 status: parameter works only for 'all' on API]
            url (str, optional): Generic part of the URL to Vid Club API. Defaults to None.

        Returns:
            Tuple[Dict[str, Any], str]: Dictionary with first response from API with JSON containing data and used URL string.

        Raises:
            ValidationError: If from_date is earlier than 2022-03-22.
            ValidationError: If to_date is earlier than from_date.
        """

        if from_date < "2022-03-22":
            raise ValidationError("from_date cannot be earlier than 2022-03-22.")

        if to_date < from_date:
            raise ValidationError("to_date cannot be earlier than from_date.")

        if url is None:
            url = self.credentials["url"]

        first_url = self.build_query(
            source=source,
            from_date=from_date,
            to_date=to_date,
            api_url=url,
            items_per_page=items_per_page,
            region=region,
        )
        headers = self.headers
        response = handle_api_response(
            url=first_url, headers=headers, method="GET", verify=False
        )
        response = response.json()

        return (response, first_url)

    def get_response(
        self,
        source: Literal["jobs", "product", "company", "survey"] = None,
        from_date: str = "2022-03-22",
        to_date: str = None,
        items_per_page: int = 100,
        region: Literal["bg", "hu", "hr", "pl", "ro", "si", "all"] = "all",
    ) -> pd.DataFrame:
        """
        Basing on the pagination type retrieved using check_connection function, gets the response from the API queried and transforms it into DataFrame.

        Args:
            source (Literal["jobs", "product", "company", "survey"], optional): The endpoint source to be accessed. Defaults to None.
            from_date (str, optional): Start date for the query, by default is the oldest date in the data 2022-03-22.
            to_date (str, optional): End date for the query. By default None, which will be executed as datetime.today().strftime("%Y-%m-%d") in code.
            items_per_page (int, optional): Number of entries per page. 100 entries by default.
            region (Literal["bg", "hu", "hr", "pl", "ro", "si", "all"], optional): Region filter for the query. Defaults to "all". [July 2023 status: parameter works only for 'all' on API]

        Returns:
            pd.DataFrame: Table of the data carried in the response.

        Raises:
            ValidationError: If any source different than the ones in the list are used.
        """
        headers = self.headers
        if source not in ["jobs", "product", "company", "survey"]:
            raise ValidationError(
                "The source has to be: jobs, product, company or survey"
            )
        if to_date == None:
            to_date = datetime.today().strftime("%Y-%m-%d")

        response, first_url = self.check_connection(
            source=source,
            from_date=from_date,
            to_date=to_date,
            items_per_page=items_per_page,
            region=region,
        )

        if isinstance(response, dict):
            keys_list = list(response.keys())
        elif isinstance(response, list):
            keys_list = list(response[0].keys())
        else:
            keys_list = []

        if "next" in keys_list:
            ind = True
        else:
            ind = False

        if "data" in keys_list:
            df = pd.DataFrame(response["data"])
            length = df.shape[0]
            page = 1

            while length == items_per_page:
                if ind == True:
                    next = response["next"]
                    url = f"{first_url}&next={next}"
                else:
                    page += 1
                    url = f"{first_url}&page={page}"
                r = handle_api_response(
                    url=url, headers=headers, method="GET", verify=False
                )
                response = r.json()
                df_page = pd.DataFrame(response["data"])
                if source == "product":
                    df_page = df_page.transpose()
                length = df_page.shape[0]
                df = pd.concat((df, df_page), axis=0)
        else:
            df = pd.DataFrame(response)

        return df

    def total_load(
        self,
        source: Literal["jobs", "product", "company", "survey"] = None,
        from_date: str = "2022-03-22",
        to_date: str = None,
        items_per_page: int = 100,
        region: Literal["bg", "hu", "hr", "pl", "ro", "si", "all"] = "all",
        days_interval: int = 30,
    ) -> pd.DataFrame:
        """
        Looping get_response and iterating by date ranges defined in intervals. Stores outputs as DataFrames in a list.
        At the end, daframes are concatenated in one and dropped duplicates that would appear when quering.

        Args:
            source (Literal["jobs", "product", "company", "survey"], optional): The endpoint source to be accessed. Defaults to None.
            from_date (str, optional): Start date for the query, by default is the oldest date in the data 2022-03-22.
            to_date (str, optional): End date for the query. By default None, which will be executed as datetime.today().strftime("%Y-%m-%d") in code.
            items_per_page (int, optional): Number of entries per page. 100 entries by default.
            region (Literal["bg", "hu", "hr", "pl", "ro", "si", "all"], optional): Region filter for the query. Defaults to "all". [July 2023 status: parameter works only for 'all' on API]
            days_interval (int, optional): Days specified in date range per api call (test showed that 30-40 is optimal for performance). Defaults to 30.

        Returns:
            pd.DataFrame: Dataframe of the concatanated data carried in the responses.
        """

        starts, ends = self.intervals(
            from_date=from_date, to_date=to_date, days_interval=days_interval
        )

        dfs_list = []
        if len(starts) > 0 and len(ends) > 0:
            for start, end in zip(starts, ends):
                logger.info(f"ingesting data for dates [{start}]-[{end}]...")
                df = self.get_response(
                    source=source,
                    from_date=start,
                    to_date=end,
                    items_per_page=items_per_page,
                    region=region,
                )
                dfs_list.append(df)
                if len(dfs_list) > 1:
                    df = pd.concat(dfs_list, axis=0, ignore_index=True)
                else:
                    df = pd.DataFrame(dfs_list[0])
        else:
            df = self.get_response(
                source=source,
                from_date=from_date,
                to_date=to_date,
                items_per_page=items_per_page,
                region=region,
            )
        df.drop_duplicates(inplace=True)

        if df.empty:
            logger.error("No data for this date range")

        return df
