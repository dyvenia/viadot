"""Vid Club Cloud API connector."""

from datetime import datetime, timedelta
from typing import Any, Dict, List, Literal, Tuple
import pandas as pd

from prefect import get_run_logger
from ..exceptions import ValidationError
from ..utils import handle_api_response
from .base import Source


logger = get_run_logger()


class VidClub(Source):
    """
    A class implementing the Vid Club API.

    Documentation for this API is located at: https://evps01.envoo.net/vipapi/
    There are 4 endpoints where to get the data.
    """

    def __init__(
        self,
        *args,
        source: Literal["jobs", "product", "company", "survey"] = None,
        from_date: str = "2022-03-22",
        to_date: str = None,
        items_per_page: int = 100,
        region: Literal["bg", "hu", "hr", "pl", "ro", "si", "all"] = None,
        days_interval: int = 30,
        cols_to_drop: List[str] = None,
        vid_club_credentials: Dict[str, Any] = None,
        validate_df_dict: dict = None,
        timeout: int = 3600,
        **kwargs
    ):
        """
        Create an instance of VidClub.

        Args:
            source (Literal["jobs", "product", "company", "survey"], optional): The
            endpoint source to be accessed. Defaults to None.
            from_date (str, optional): Start date for the query, by default is the
            oldest date in the data 2022-03-22.
            to_date (str, optional): End date for the query. By default None,
                which will be executed as datetime.today().strftime("%Y-%m-%d") in code.
            items_per_page (int, optional): Number of entries per page. Defaults to 100.
            region (Literal["bg", "hu", "hr", "pl", "ro", "si", "all"], optional):
                Region filter for the query. Defaults to None
                (parameter is not used in url). [December 2023 status: value 'all'
                does not work for company and jobs]
            days_interval (int, optional): Days specified in date range per API call
                (test showed that 30-40 is optimal for performance). Defaults to 30.
            cols_to_drop (List[str], optional): List of columns to drop.
                Defaults to None.
            vid_club_credentials (Dict[str, Any], optional): Stores the credentials
                information. Defaults to None.
            validate_df_dict (dict, optional): A dictionary with optional list of tests
                to verify the output
                dataframe. If defined, triggers the `validate_df` task from task_utils.
                Defaults to None.
            timeout (int, optional): The time (in seconds) to wait while running this
                task before a timeout occurs. Defaults to 3600.
        """
        self.source = source
        self.from_date = from_date
        self.to_date = to_date
        self.items_per_page = items_per_page
        self.region = region
        self.days_interval = days_interval
        self.cols_to_drop = cols_to_drop
        self.vid_club_credentials = vid_club_credentials
        self.validate_df_dict = validate_df_dict
        self.timeout = timeout

        self.headers = {
            "Authorization": "Bearer " + vid_club_credentials["token"],
            "Content-Type": "application/json",
        }

        super().__init__(*args, **kwargs)

    def build_query(
        self,
        from_date: str,
        to_date: str,
        api_url: str,
        items_per_page: int,
        source: Literal["jobs", "product", "company", "survey"] = None,
        region: Literal["bg", "hu", "hr", "pl", "ro", "si", "all"] = None,
    ) -> str:
        """
        Builds the query from the inputs.

        Args:
            from_date (str): Start date for the query.
            to_date (str): End date for the query, if empty, will be executed as
                datetime.today().strftime("%Y-%m-%d").
            api_url (str): Generic part of the URL to Vid Club API.
            items_per_page (int): number of entries per page.
            source (Literal["jobs", "product", "company", "survey"], optional):
                The endpoint source to be accessed. Defaults to None.
            region (Literal["bg", "hu", "hr", "pl", "ro", "si", "all"], optional):
                Region filter for the query. Defaults to None
                (parameter is not used in url). [December 2023 status: value 'all'
                does not work for company and jobs]

        Returns:
            str: Final query with all filters added.

        Raises:
            ValidationError: If any source different than the ones in the list are used.
        """
        if source in ["jobs", "product", "company"]:
            region_url_string = f"&region={region}" if region else ""
            url = f"""{api_url}{source}?from={from_date}&to={to_date}
            {region_url_string}&limit={items_per_page}"""
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
            to_date (str): End date for the query, if empty, will be executed as
                datetime.today().strftime("%Y-%m-%d").
            days_interval (int): Days specified in date range per api call
                (test showed that 30-40 is optimal for performance).

        Returns:
            List[str], List[str]: Starts and Ends lists that contains information
                about date ranges for specific period and time interval.

        Raises:
            ValidationError: If the final date of the query is before the start date.
        """
        if to_date is None:
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
        region: Literal["bg", "hu", "hr", "pl", "ro", "si", "all"] = None,
        url: str = None,
    ) -> Tuple[Dict[str, Any], str]:
        """
        Initiate first connection to API to retrieve piece of data.

        With information about type of pagination in API URL.
        This option is added because type of pagination for endpoints is being changed
        in the future from page number to 'next' id.

        Args:
            source (Literal["jobs", "product", "company", "survey"], optional):
                The endpoint source to be accessed. Defaults to None.
            from_date (str, optional): Start date for the query, by default is the
                oldest date in the data 2022-03-22.
            to_date (str, optional): End date for the query. By default None,
                which will be executed as datetime.today().strftime("%Y-%m-%d") in code.
            items_per_page (int, optional): Number of entries per page.
                100 entries by default.
            region (Literal["bg", "hu", "hr", "pl", "ro", "si", "all"], optional):
                Region filter for the query. Defaults to None
                (parameter is not used in url). [December 2023 status: value 'all'
                does not work for company and jobs]
            url (str, optional): Generic part of the URL to Vid Club API.
                Defaults to None.

        Returns:
            Tuple[Dict[str, Any], str]: Dictionary with first response from API with
                JSON containing data and used URL string.

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
            url=first_url, headers=headers, method="GET"
        )
        response = response.json()
        return (response, first_url)

    def get_response(
        self,
        source: Literal["jobs", "product", "company", "survey"] = None,
        from_date: str = "2022-03-22",
        to_date: str = None,
        items_per_page: int = 100,
        region: Literal["bg", "hu", "hr", "pl", "ro", "si", "all"] = None,
    ) -> pd.DataFrame:
        """
        Basing on the pagination type retrieved using check_connection function.

        It gets the response from the API queried and transforms it into DataFrame.

        Args:
            source (Literal["jobs", "product", "company", "survey"], optional):
                The endpoint source to be accessed. Defaults to None.
            from_date (str, optional): Start date for the query, by default is the
                oldest date in the data 2022-03-22.
            to_date (str, optional): End date for the query. By default None,
                which will be executed as datetime.today().strftime("%Y-%m-%d") in code.
            items_per_page (int, optional): Number of entries per page.
                100 entries by default.
            region (Literal["bg", "hu", "hr", "pl", "ro", "si", "all"], optional):
                Region filter for the query. Defaults to None
                (parameter is not used in url). [December 2023 status: value 'all'
                does not work for company and jobs]

        Returns
            pd.DataFrame: Table of the data carried in the response.

        Raises:
            ValidationError: If any source different than the ones in the list are used.
        """
        headers = self.headers
        if source not in ["jobs", "product", "company", "survey"]:
            raise ValidationError(
                "The source has to be: jobs, product, company or survey"
            )
        if to_date is None:
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

        ind = "next" in keys_list

        if "data" in keys_list:
            df = pd.json_normalize(response["data"])
            df = pd.DataFrame(df)
            length = df.shape[0]
            page = 1

            while length == items_per_page:
                if ind is True:
                    next_page = response["next"]
                    url = f"{first_url}&next={next_page}"
                else:
                    page += 1
                    url = f"{first_url}&page={page}"
                response_api = handle_api_response(
                    url=url, headers=headers, method="GET"
                )
                response = response_api.json()
                df_page = pd.json_normalize(response["data"])
                df_page = pd.DataFrame(df_page)
                if source == "product":
                    df_page = df_page.transpose()
                length = df_page.shape[0]
                df = pd.concat((df, df_page), axis=0)
        else:
            df = pd.DataFrame(response)

        return df

    def to_df(
        self,
        if_empty: str = "warn",
    ) -> pd.DataFrame:
        """
        Looping get_response and iterating by date ranges defined in intervals.

        Stores outputs as DataFrames in a list. At the end, daframes are concatenated
        in one and dropped duplicates that would appear when quering.

        Args:
            if_empty (str, optional): What to do if a fetch produce no data.
                Defaults to "warn

        Returns:
            pd.DataFrame: Dataframe of the concatanated data carried in the responses.
        """
        starts, ends = self.intervals(
            from_date=self.from_date,
            to_date=self.to_date,
            days_interval=self.days_interval
        )

        dfs_list = []
        if len(starts) > 0 and len(ends) > 0:
            for start, end in zip(starts, ends):
                logger.info(f"ingesting data for dates [{start}]-[{end}]...")
                df = self.get_response(
                    source=self.source,
                    from_date=start,
                    to_date=end,
                    items_per_page=self.items_per_page,
                    region=self.region,
                )
                dfs_list.append(df)
                if len(dfs_list) > 1:
                    df = pd.concat(dfs_list, axis=0, ignore_index=True)
                else:
                    df = pd.DataFrame(dfs_list[0])
        else:
            df = self.get_response(
                source=self.source,
                from_date=self.from_date,
                to_date=self.to_date,
                items_per_page=self.items_per_page,
                region=self.region,
            )
        list_columns = df.columns[
            df.map(lambda x: isinstance(x, list)).any()
        ].tolist()
        for i in list_columns:
            df[i] = df[i].apply(lambda x: tuple(x) if isinstance(x, list) else x)
        df.drop_duplicates(inplace=True)

        if self.cols_to_drop is not None:
            if isinstance(self.cols_to_drop, list):
                try:
                    logger.info(f"Dropping following columns: {self.cols_to_drop}...")
                    df.drop(
                        columns=self.cols_to_drop, inplace=True, errors="raise"
                    )
                except KeyError:
                    logger.error(
                        f"""Column(s): {self.cols_to_drop} don't exist in the DataFrame.
                        No columns were dropped. Returning full DataFrame..."""
                    )
                    logger.info(f"Existing columns: {df.columns}")
            else:
                raise TypeError("Provide columns to drop in a List.")

        if df.empty:
            logger.error("No data for this date range")

        return df
