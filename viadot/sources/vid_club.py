import json
import os
import urllib
from datetime import datetime, date, timedelta
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

    def __init__(
        self, 
        *args, 
        credentials: Dict[str, Any] = None, 
        **kwargs):
        """
        Create an instance of VidClub.

        Args:
            credentials (Dict[str, Any], optional): Credentials to Vid Club APIs containing token.
                Defaults to dictionary.

        Raises:
            CredentialError: If credentials are not provided as a parameter.
        """
        if credentials is not None:
            self.credentials = credentials
        else:
            raise CredentialError("Credentials not provided.")

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
        page_param: bool = True
    ) -> str:
        """
        Builds the query from the inputs.

        Args:
            from_date (str): Start date for the query.
            to_date (str): End date for the query, if empty, datetime.today() will be used.
            api_url (str): Generic part of the URL.
            items_per_page (int): number of entries per page.
            source (Literal["jobs", "product", "company", "survey"], optional): The endpoint source to be accessed. Defaults to None.
            page_param (bool, optional): 

        Returns:
            str: Final query with all filters added.

        Raises:
            ValidationError: If any source different than the ones in the list are used.
        """
        if source in ["jobs", "product", "company"]:
            url = f"{api_url}{source}?from={from_date}&to={to_date}&limit={items_per_page}"
        elif source in "survey":
            url = f"{api_url}{source}?language=en&type=question"
        else:
            raise ValidationError(
                "Pick one these sources: jobs, product, company, survey"
            )
        return url

    def intervals(
        self,
        from_date: str,
        to_date: str,
        days_interval: int
    ) -> Tuple[List[str], List[str]]:
        """Breaks dates range into smaller by provided days interval.

        Args:
            from_date (str): Start date for the query.
            to_date (str): End date for the query. By default, datetime.today() will be used.
            days_interval (int): Days specified in date range per api call (test showed that 30-40 is optimal for performance).

        Returns:
            List[str], List[str]: Starts abd Ends lists that contains information about date ranges for specific period and time interval.
        """

        if to_date == None:
            end_date = datetime.today().date()
        else:
            end_date = datetime.strptime(to_date, "%Y-%m-%d").date()
        start_date = datetime.strptime(from_date, "%Y-%m-%d").date()
        interval = timedelta(days=days_interval)
        starts = []
        ends = []
        period_start = start_date
        while period_start < end_date:
            period_end = min(period_start + interval, end_date)
            starts.append(period_start.strftime("%Y-%m-%d"))
            ends.append(period_end.strftime("%Y-%m-%d"))
            period_start = period_end

        return starts, ends

    def get_response(
        self,
        source: Literal["jobs", "product", "company", "survey"] = None,
        from_date: str = "2022-03-22",
        to_date: str = None,
        items_per_page: int = 100,
        region="null",
    ) -> pd.DataFrame:
        """
        Gets the response from the API queried and transforms it into DataFrame.

        Args:
            source (Literal["jobs", "product", "company", "survey"], optional): The endpoint source to be accessed. Defaults to None.
            from_date (str, optional): Start date for the query, by default is the oldest date in the data 2022-03-22.
            to_date (str, optional): End date for the query. By default, datetime.today() will be used.
            items_per_page (int, optional): Number of entries per page. 100 entries by default.
            region (str, optinal): Region filter for the query. By default, it is empty.

        Returns:
            pd.DataFrame: Table of the data carried in the response.

        Raises:
            ValidationError: If any source different than the ones in the list are used.
            ValidationError: If the initial date of the query is before the oldest date in the data (2022-03-22).
            ValidationError: If the final date of the query is before the start date.
        """

        if source not in ["jobs", "product", "company", "survey"]:
            raise ValidationError(
                "The source has to be: jobs, product, company or survey"
            )
        if to_date == None:
            to_date = datetime.today().strftime("%Y-%m-%d")

        from_date_obj = datetime.strptime(from_date, "%Y-%m-%d")
        oldest_date_obj = datetime.strptime("2022-03-22", "%Y-%m-%d")
        delta = from_date_obj - oldest_date_obj

        if delta.days < 0:
            raise ValidationError("from_date cannot be earlier than 2022-03-22.")

        to_date_obj = datetime.strptime(to_date, "%Y-%m-%d")
        delta = to_date_obj - from_date_obj

        if delta.days < 0:
            raise ValidationError("to_date cannot be earlier than from_date.")

        first_url = self.build_query(
            source = source,

            from_date = from_date,
            to_date = to_date,
            api_url = self.credentials["url"],
            items_per_page=items_per_page,
        )
        headers = self.headers

        response = handle_api_response(url=first_url, headers=headers, method="GET")

        response = response.json()

        if isinstance(response, dict):
            keys_list = list(response.keys())
        elif isinstance(response, list):
            keys_list = list(response[0].keys())
        else:
            keys_list = []

        if "data" in keys_list:
            df = pd.DataFrame(response["data"])
            length = df.shape[0]
            page = 2

            while length == items_per_page:
                url = f"{first_url}&page={page}"
                r = handle_api_response(url=url, headers=headers, method="GET")
                response = r.json()
                df_page = pd.DataFrame(response["data"])
                if source == "product":
                    df_page = df_page.transpose()
                length = df_page.shape[0]
                df = pd.concat((df, df_page), axis=0)
                page += 1

        else:
            df = pd.DataFrame(response)

        return df

    def total_load(
        self,
        source: Literal["jobs", "product", "company", "survey"] = None,
        from_date: str = "2022-03-22",
        to_date: str = None,
        items_per_page: int = 100,
        region: str = "null",
        days_interval: int = 30
    ) -> pd.DataFrame:
        """Creating date ranges for provided time interval by which get_response is looped and stores dataframes in a list. 

        Args:
            source (Literal["jobs", "product", "company", "survey"], optional): The endpoint source to be accessed. Defaults to None.
            from_date (str, optional): Start date for the query, by default is the oldest date in the data 2022-03-22.
            to_date (str, optional): End date for the query. By default, datetime.today() will be used.
            items_per_page (int, optional): Number of entries per page. 100 entries by default.
            region (str, optinal): Region filter for the query. By default, it is empty.
            days_interval (int, optional): Days specified in date range per api call (test showed that 30-40 is optimal for performance). Defaults to 30.

        Returns:
            pd.DataFrame: Dataframe of the concatanated data carried in the responses.
        """

        starts, ends = self.intervals(
            from_date = from_date,
            to_date = to_date,
            days_interval = days_interval
        )

        dfs_list = []
        if len(starts) > 0 and len(ends) > 0: 
            for start, end in zip(starts, ends):
                logger.info(f"ingesting data for dates {start}-{end}...")
                df = self.get_response(
                    source = source,
                    from_date = start,
                    to_date = end,
                    items_per_page = items_per_page,
                    region = region,
                )
                dfs_list.append(df)
                if len(dfs_list) > 1:
                    logger.info("Concatanating tables into one dataframe...")
                    df = pd.concat(dfs_list, axis=0, ignore_index=True)
                else:
                    df = pd.DataFrame(dfs_list[0])              
        else:
            df = self.get_response(
                    source = source,
                    from_date = from_date,
                    to_date = to_date,
                    items_per_page = items_per_page,
                    region = region,
                )
        df.drop_duplicates(inplace = True)
        
        if df.empty:
            logger.error("No data for this date range")
            
        return df