"""
'customer_gauge.py'.

Structure for the Customer Gauge Cloud API connector.

This module provides functionalities for connecting to Customer Gauge Cloud API
and download the reports generated.


CustomerGauge Class Attributes:

    endpoint (Literal["responses", "non-responses"]):
        Indicate which endpoint to connect. Defaults to None.
    endpoint_url (str, optional): Endpoint URL. Defaults to None.
    total_load (bool, optional): Indicate whether to download
        the data to the latest. If 'False', only one API call is executed
        (up to 1000 records). Defaults to True.
    cursor (int, optional): Cursor value to navigate to the page.
        Defaults to None.
    pagesize (int, optional): Number of responses (records)
        returned per page, max value = 1000. Defaults to 1000.
    date_field (Literal["date_creation", "date_order", "date_sent",
        "date_survey_response"], optional): Specifies the date type
        which filter date range. Defaults to None.
    start_date (datetime, optional): Defines the period end date in
        yyyy-mm-dd format. Defaults to None.
    end_date (datetime, optional): Defines the period start date in
        yyyy-mm-dd format. Defaults to None.
    unpack_by_field_reference_cols (List[str]): Columns to unpack and modify
        using `_field_reference_unpacker`. Defaults to None.
    unpack_by_nested_dict_transformer (List[str]): Columns to unpack and modify
        using `_nested_dict_transformer`. Defaults to None.
    credentials (Dict[str, Any], optional): Credentials to connect with API
        containing client_id, client_secret. Defaults to None.
    anonymize (bool, optional): Indicates if anonymize selected columns.
        Defaults to False.
    columns_to_anonymize (List[str], optional): List of columns to anonymize.
        Defaults to None.
    anonymize_method  (Literal["mask", "hash"], optional): Method of
        anonymizing data. "mask" -> replace the data with "value" arg. "hash" ->
        replace the data with the hash value of an object (using `hash()`
        method). Defaults to "mask".
    anonymize_value (str, optional): Value to replace the data.
        Defaults to "***".
    date_column (str, optional): Name of the date column used to identify rows
        that are older than a specified number of days. Defaults to None.
    days (int, optional): The number of days beyond which we want to anonymize
        the data, e.g. older than 2 years can be: 2*365. Defaults to None.
    validate_df_dict (Dict[str], optional): A dictionary with optional list of
        tests to verify the output dataframe. If defined, triggers the
        `validate_df` task from task_utils. Defaults to None.
    timeout (int, optional): The time (in seconds) to wait while running this
        task before a timeout occurs. Defaults to 3600.

Functions:
    get_token(self): Gets Bearer Token using POST request method.
    get_json_response(self, cursor, pagesize, date_field, start_date, end_date): Gets
        JSON with nested structure that contains data and cursor parameter value.
    get_cursor(self, json_response): Returns cursor value that is needed to navigate
        to the next page.
    get_data(self, json_response): Extract and return the 'data' part of a JSON
        response as a list of dictionaries.
    _field_reference_unpacker(self, json_response, field): Unpack and modify
        dictionaries within the specified field of a JSON response.
    _nested_dict_transformer(self, json_response, field): Modify nested dictionaries
        within the specified field of a JSON response.
    column_unpacker(self, json_list, unpack_by_field_reference_cols,
        unpack_by_nested_dict_transformer): Function to unpack and modify specific
        columns in a list of dictionaries.
    flatten_json(self, json_response): Function that flattens a nested structure
        of the JSON object.
    square_brackets_remover(self, df): Replace square brackets "[]" with an empty
        string in a pandas DataFrame.
    _drivers_cleaner(self, drivers): Clean and format the 'drivers' data.
    to_df(self, if_empty): Downloading the selected range
        of data from Customer Gauge endpoint.

Classes:
    CustomerGauge: Class implementing the CustomerGauge API.
"""

from datetime import datetime
from typing import Any, Dict, List, Literal

import pandas as pd
from prefect import get_run_logger

from viadot.exceptions import APIError, CredentialError
from viadot.orchestration.prefect.tasks.task_utils import anonymize_df, validate_df
from viadot.sources.base import Source
from viadot.utils import add_viadot_metadata_columns, handle_api_response

logger = get_run_logger()


class CustomerGauge(Source):
    """A class to connect and download data using Customer Gauge API."""

    API_URL = "https://api.eu.customergauge.com/v7/rest/sync/"

    def __init__(
        self,
        *args,
        endpoint: Literal["responses", "non-responses"] = None,
        endpoint_url: str = None,
        total_load: bool = True,
        cursor: int = None,
        pagesize: int = 1000,
        date_field: Literal[
            "date_creation", "date_order", "date_sent", "date_survey_response"
        ] = None,
        start_date: datetime = None,
        end_date: datetime = None,
        unpack_by_field_reference_cols: List[str] = None,
        unpack_by_nested_dict_transformer: List[str] = None,
        credentials: Dict[str, Any] = None,
        anonymize: bool = False,
        columns_to_anonymize: List[str] = None,
        anonymize_method: Literal["mask", "hash"] = "mask",
        anonymize_value: str = "***",
        date_column: str = None,
        days: int = None,
        validate_df_dict: dict = None,
        timeout: int = 3600,
        **kwargs,
    ):
        """
        A class to connect and download data using Customer Gauge API.

        Below is the documentation for each of this API's gateways:
            Responses gateway https://support.customergauge.com/support/solutions/
                articles/5000875861-get-responses
            Non-Responses gateway https://support.customergauge.com/support/solutions/
                articles/5000877703-get-non-responses

        Args:
            endpoint (Literal["responses", "non-responses"]):
                Indicate which endpoint to connect. Defaults to None.
            total_load (bool, optional): Indicate whether to download
                the data to the latest. If 'False', only one API call is executed
                (up to 1000 records). Defaults to True.
            endpoint_url (str, optional): Endpoint URL. Defaults to None.
            cursor (int, optional): Cursor value to navigate to the page.
                Defaults to None.
            pagesize (int, optional): Number of responses (records)
                returned per page, max value = 1000. Defaults to 1000.
            date_field (Literal["date_creation", "date_order", "date_sent",
                "date_survey_response"], optional): Specifies the date type
                which filter date range. Defaults to None.
            start_date (datetime, optional): Defines the period end date in
                yyyy-mm-dd format. Defaults to None.
            end_date (datetime, optional): Defines the period start date in
                yyyy-mm-dd format. Defaults to None.
            unpack_by_field_reference_cols (List[str]): Columns to unpack and modify
                using `_field_reference_unpacker`. Defaults to None.
            unpack_by_nested_dict_transformer (List[str]): Columns to unpack and modify
                using `_nested_dict_transformer`. Defaults to None.
            credentials (Dict[str, Any], optional): Credentials to connect with API
                containing client_id, client_secret. Defaults to None.
            anonymize (bool, optional): Indicates if anonymize selected columns.
                Defaults to False.
            columns_to_anonymize (List[str], optional): List of columns to anonymize.
                Defaults to None.
            anonymize_method  (Literal["mask", "hash"], optional): Method of
                anonymizing data. "mask" -> replace the data with "value" arg. "hash" ->
                replace the data with the hash value of an object (using `hash()`
                method). Defaults to "mask".
            anonymize_value (str, optional): Value to replace the data.
                Defaults to "***".
            date_column (str, optional): Name of the date column used to identify rows
                that are older than a specified number of days. Defaults to None.
            days (int, optional): The number of days beyond which we want to anonymize
                the data, e.g. older than 2 years can be: 2*365. Defaults to None.
            validate_df_dict (Dict[str], optional): A dictionary with optional list of
                tests to verify the output dataframe. If defined, triggers the
                `validate_df` task from task_utils. Defaults to None.
            timeout (int, optional): The time (in seconds) to wait while running this
                task before a timeout occurs. Defaults to 3600.
        Raises:
            ValueError: If endpoint is not provided or incorrect.
            CredentialError: If credentials are not provided
                in local_config or directly as a parameter
        """
        self.endpoint = endpoint

        if endpoint_url is not None:
            self.url = endpoint_url
        elif endpoint is not None:
            if endpoint in ["responses", "non-responses"]:
                self.url = f"{self.API_URL}{endpoint}"
            else:
                raise ValueError(
                    """Incorrect endpoint name. Choose: 'responses' or
                    'non-responses'"""
                )
        else:
            raise ValueError(
                """Provide endpoint name. Choose: 'responses' or 'non-responses'.
                Otherwise, provide URL"""
            )

        if credentials is not None:
            self.credentials = credentials
        else:
            raise CredentialError("Credentials not provided.")

        self.total_load = total_load
        self.cursor = cursor
        self.pagesize = pagesize
        self.date_field = date_field
        self.start_date = start_date
        self.end_date = end_date
        self.unpack_by_field_reference_cols = unpack_by_field_reference_cols
        self.unpack_by_nested_dict_transformer = unpack_by_nested_dict_transformer
        self.anonymize = anonymize
        self.columns_to_anonymize = columns_to_anonymize
        self.anonymize_method = anonymize_method
        self.anonymize_value = anonymize_value
        self.date_column = date_column
        self.days = days
        self.validate_df_dict = validate_df_dict
        self.timeout = timeout

        super().__init__()

    def get_token(self) -> str:
        """
        Gets Bearer Token using POST request method.

        Raises:
            APIError: If token is not returned.

        Returns:
            str: Bearer Token value.
        """
        url = "https://auth.EU.customergauge.com/oauth2/token"
        client_id = self.credentials.get("client_id", None)
        client_secret = self.credentials.get("client_secret", None)

        headers = {"Content-Type": "application/x-www-form-urlencoded"}
        body = {
            "grant_type": "client_credentials",
            "client_id": client_id,
            "client_secret": client_secret,
        }
        api_response = handle_api_response(
            url=url, params=body, headers=headers, method="POST"
        )
        token = api_response.json().get("access_token")

        if token is None:
            raise APIError("The token could not be generated. Check your credentials.")

        return token

    def get_json_response(
        self,
        cursor: int = None,
        pagesize: int = 1000,
        date_field: Literal[
            "date_creation", "date_order", "date_sent", "date_survey_response"
        ] = None,
        start_date: datetime = None,
        end_date: datetime = None,
    ) -> Dict[str, Any]:
        """
        Gets JSON with nested structure that contains data and cursor parameter value.

        It is using GET request method.

        Args:
            cursor (int, optional): Cursor value to navigate to the page.
                Defaults to None.
            pagesize (int, optional): Number of responses (records) returned per page,
                max value = 1000. Defaults to 1000. Defaults to 1000.
            date_field (Literal["date_creation", "date_order", "date_sent",
                "date_survey_response"], optional): Specifies the date type which
                filter date range. Defaults to None.
            start_date (datetime, optional): Defines the period start date
                in yyyy-mm-dd format. Defaults to None.
            end_date (datetime, optional): Defines the period end date
                in yyyy-mm-dd format. Defaults to None.

        Raises:
            ValueError: If at least one date argument were provided
                and the rest is missing. Needed all 3 or skip them.
            APIError: If no response from API call.

        Returns:
            Dict[str, Any]: JSON with data and cursor parameter value.
        """
        params = {
            "per_page": pagesize,
            "with[]": ["drivers", "tags", "questions", "properties"],
            "cursor": cursor,
        }

        if any([date_field, start_date, end_date]):
            if all([date_field, start_date, end_date]):
                params["period[field]"] = (date_field,)
                params["period[start]"] = (start_date,)
                params["period[end]"] = end_date
            else:
                raise ValueError(
                    """Missing date arguments: 'date_field', 'start_date', 'end_date'.
                    Provide all 3 arguments or skip all of them."""
                )

        header = {"Authorization": f"Bearer {self.get_token()}"}
        api_response = handle_api_response(url=self.url, headers=header, params=params)
        response = api_response.json()

        if response is None:
            raise APIError("No response.")
        return response

    def get_cursor(self, json_response: Dict[str, Any] = None) -> int:
        """
        Returns cursor value that is needed to navigate to the next page.

        In the next API call for specific pagesize.

        Args:
            json_response (Dict[str, Any], optional): Dictionary with nested structure
                that contains data and cursor parameter value. Defaults to None.

        Raises:
            ValueError: If cursor value not found.

        Returns:
            int: Cursor value.
        """
        try:
            cur = json_response["cursor"]["next"]
        except KeyError as error:
            raise ValueError(
                """Provided argument doesn't contain 'cursor' value.
                Pass json returned from the endpoint."""
            ) from error

        return cur

    def get_data(
        self,
        json_response: Dict[str, Any] = None,
    ) -> List[Dict[str, Any]]:
        """
        Extract and return the 'data' part of a JSON response as a list of dictionaries.

        Args:
            json_response (Dict[str, Any], optional): JSON object represented as a
                nested dictionary that contains data and cursor parameter value.
                Defaults to None.

        Raises:
            KeyError: If the 'data' key is not present in the provided JSON response.

        Returns:
            List[Dict[str, Any]]: A list of dictionaries containing data from the 'data'
            part of the JSON response.
        """
        jsons_list = []
        try:
            jsons_list = json_response["data"]
        except KeyError:
            logger.error(
                """Provided argument doesn't contain 'data' value. Pass json returned
                from the endpoint."""
            )
            raise

        return jsons_list

    def _field_reference_unpacker(
        self,
        json_response: Dict[str, Any],
        field: str,
    ) -> Dict[str, Any]:
        """
        Unpack and modify dictionaries within the specified field of a JSON response.

        This function takes a JSON response and a field name. It processes dictionaries
        within the specified field, checking if each dictionary contains
        exactly two items. If a dictionary meets this criteria, it is transformed
        into a new dictionary, where the first key becomes a key, and the second key
        becomes its associated value

        Args:
            json_response (Dict[str, Any], optional): JSON response with data.
            field (str): The key (column) of the dictionary to be modified.

        Returns:
            Dict[str, Any]: The JSON response with modified nested dictionaries
            within the specified field.

        Raises:
            ValueError: If a dictionary within the specified field doesn't contain
                exactly two items.
        """
        result = {}
        for _i, dictionary in enumerate(json_response[field]):
            if isinstance(dictionary, dict) and len(dictionary.items()) == 2:
                list_properties = list(dictionary.values())
                result[list_properties[0]] = list_properties[1]
            else:
                raise ValueError(
                    """Dictionary within the specified field doesn't contain
                    exactly two items."""
                )
        if result:
            json_response[field] = result

        return json_response

    def _nested_dict_transformer(
        self,
        json_response: Dict[str, Any],
        field: str,
    ) -> Dict[str, Any]:
        """
        Modify nested dictionaries within the specified field of a JSON response.

        This function takes a JSON response and a field name. It modifies nested
        dictionaries within the specified field by adding an index and underscore
        to the keys. The modified dictionary is then updated in the JSON response.

        Args:
            json_response (Dict[str, Any], optional): JSON response with data.
            field (str): The key (column) of the dictionary to be modified.

        Returns:
            Dict[str, Any]: The JSON response with modified nested dictionaries
        within the specified field.
        """
        result = {}
        try:
            for i, dictionary in enumerate(json_response[field], start=1):
                for key, value in dictionary.items():
                    result[f"{i}_{key}"] = value
            if result:
                json_response[field] = result
        except TypeError as type_error:
            logger.error(type_error)

        return json_response

    def column_unpacker(
        self,
        json_list: List[Dict[str, Any]] = None,
        unpack_by_field_reference_cols: List[str] = None,
        unpack_by_nested_dict_transformer: List[str] = None,
    ) -> List[Dict[str, Any]]:
        """
        Function to unpack and modify specific columns in a list of dictionaries.

        It is using one of two methods, chosen by the user.
        If user would like to use field_reference_unpacker, he/she needs to provide
        list of fields as strings in `unpack_by_field_reference_cols`  parameter,
        if user would like to use nested_dict_transformer he/she needs to provide
        list of fields as strings in unpack_by_nested_dict_transformer parameter.

        Args:
            json_list (List[Dict[str, Any]): A list of dictionaries containing the data.
            unpack_by_field_reference_cols (List[str]): Columns to unpack and modify
                using `_field_reference_unpacker`. Defaults to None.
            unpack_by_nested_dict_transformer (List[str]): Columns to unpack and
                modify using `_nested_dict_transformer`. Defaults to None.

        Raises:
            ValueError: If 'json_list' is not provided.
            ValueError: If specified columns do not exist in the JSON data.
            ValueError: If columns are mentioned in both
                'unpack_by_field_reference_cols' and 'unpack_by_nested_dict_transformer'

        Returns:
            List[Dict[str, Any]]: The updated list of dictionaries after column
                unpacking and modification.
        """
        duplicated_cols = []

        if json_list is None:
            raise ValueError("Input 'json_list' is required.")

        def unpack_columns(columns, unpack_function):
            json_list_clean = json_list.copy()
            for field in columns:
                if field in json_list_clean[0]:
                    logger.info(
                        f"""Unpacking column '{field}' with {unpack_function.__name__}
                        method..."""
                    )
                    try:
                        json_list_clean = list(
                            map(
                                lambda x, f=field: unpack_function(x, f),
                                json_list_clean,
                            )
                        )
                        logger.info(
                            f"All elements in '{field}' are unpacked successfully."
                        )
                    except ValueError:
                        logger.info(
                            f"No transformation were made in '{field}',"
                            "because didn't contain list of key-value data."
                        )
                else:
                    logger.info(f"Column '{field}' not found.")
            return json_list_clean

        if unpack_by_field_reference_cols and unpack_by_nested_dict_transformer:
            duplicated_cols = set(unpack_by_field_reference_cols).intersection(
                set(unpack_by_nested_dict_transformer)
            )
        if duplicated_cols:
            raise ValueError(
                f"""{duplicated_cols} were mentioned in both
                unpack_by_field_reference_cols and unpack_by_nested_dict_transformer.
                It's not possible to apply two methods to the same field."""
            )

        if unpack_by_field_reference_cols is not None:
            json_list = unpack_columns(
                columns=unpack_by_field_reference_cols,
                unpack_function=self._field_reference_unpacker,
            )

        if unpack_by_nested_dict_transformer is not None:
            json_list = unpack_columns(
                columns=unpack_by_nested_dict_transformer,
                unpack_function=self._nested_dict_transformer,
            )

        return json_list

    def flatten_json(self, json_response: Dict[str, Any] = None) -> Dict[str, Any]:
        """
        Function that flattens a nested structure of the JSON object.

        Function flattens it into a single-level dictionary. It uses a nested
        `flattify()` function to recursively combine nested keys in the JSON
        object with '_' to create the flattened keys.

        Args:
            json_response (Dict[str, Any], optional): JSON object represented as
            a nested dictionary. Defaults to None.

        Raises:
            TypeError: If the 'json_response' not a dictionary.

        Returns:
            Dict[str, Any]: The flattened dictionary.
        """
        result = {}

        if not isinstance(json_response, dict):
            raise TypeError("Input must be a dictionary.")

        def flattify(field, key="", out=None):
            if out is None:
                out = result

            if isinstance(field, dict):
                for item in field.keys():
                    flattify(field[item], key + item + "_", out)
            else:
                out[key[:-1]] = field

        flattify(json_response)

        return result

    def square_brackets_remover(self, df: pd.DataFrame = None) -> pd.DataFrame:
        """
        Replace square brackets "[]" with an empty string in a pandas DataFrame.

        Args:
            df (pd.DataFrame, optional): Replace square brackets "[]" with an empty
                string in a pandas DataFrame. Defaults to None.

        Returns:
            pd.DataFrame: The modified DataFrame with square brackets replaced by
                an empty string.
        """
        df = df.astype(str)
        df = df.map(lambda x: x.strip("[]"))
        return df

    def _drivers_cleaner(self, drivers: str = None) -> str:
        """
        Clean and format the 'drivers' data.

        Args:
            drivers (str, optional): Column name of the data to be cleaned.
                Defaults to None.

        Returns:
            str: A cleaned and formatted string of driver data.
        """
        cleaned_drivers = (
            drivers.replace("{", "")
            .replace("}", "")
            .replace("'", "")
            .replace("label: ", "")
        )

        return cleaned_drivers

    @add_viadot_metadata_columns
    def to_df(self, if_empty: str = "warn") -> pd.DataFrame:
        """
        Downloading the selected range of data from Customer Gauge endpoint.

        Return one pandas DataFrame.

        Returns:
            pd.DataFrame: Final pandas DataFrame.
        """
        total_json = []

        logger.info(
            f"""Starting downloading data from {self.endpoint or self.url}
                endpoint..."""
        )
        json_data = self.get_json_response(
            cursor=self.cursor,
            pagesize=self.pagesize,
            date_field=self.date_field,
            start_date=self.start_date,
            end_date=self.end_date,
        )
        cur = self.get_cursor(json_data)

        jsn = self.get_data(json_data)
        total_json += jsn

        if self.total_load:
            if self.cursor is None:
                logger.info(
                    f"""Downloading all the data from the
                    {self.endpoint or self.url} endpoint.
                    "Process might take a few minutes..."""
                )
            else:
                logger.info(
                    f"""Downloading starting from the {self.cursor} cursor.
                    Process might take a few minutes..."""
                )
            while jsn:
                json_data = self.get_json_response(cursor=cur)
                cur = self.get_cursor(json_data)
                jsn = self.get_data(json_data)
                total_json += jsn

        clean_json = self.column_unpacker(
            json_list=total_json,
            unpack_by_field_reference_cols=self.unpack_by_field_reference_cols,
            unpack_by_nested_dict_transformer=self.unpack_by_nested_dict_transformer,
        )
        logger.info("Inserting data into the DataFrame...")
        df = pd.DataFrame(list(map(self.flatten_json, clean_json)))
        df = self.square_brackets_remover(df)
        if "drivers" in list(df.columns):
            df["drivers"] = df["drivers"].apply(self._drivers_cleaner)
        df.columns = df.columns.str.lower().str.replace(" ", "_")

        #  Running Data Frame validation if tests dictionary was specified
        if self.validate_df_dict is not None:
            validate_df(df=df, tests=self.validate_df_dict)

        #  Running Data Frame anonymization if this option is set on True
        if self.anonymize:
            df = anonymize_df(
                df=df,
                columns=self.columns_to_anonymize,
                method=self.anonymize_method,
                value=self.anonymize_value,
                date_column=self.date_column,
                days=self.days,
            )
            logger.info("Data Frame anonymized")

        logger.info("DataFrame: Ready. Data: Inserted. Let the magic happen!")

        return df
