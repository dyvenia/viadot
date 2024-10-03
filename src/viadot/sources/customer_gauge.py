"""'customer_gauge.py'."""

from datetime import datetime
from typing import Any, Literal

import pandas as pd
from pydantic import BaseModel

from viadot.config import get_source_credentials
from viadot.exceptions import APIError
from viadot.sources.base import Source
from viadot.utils import (
    add_viadot_metadata_columns,
    anonymize_df,
    handle_api_response,
    validate,
)


class CustomerGaugeCredentials(BaseModel):
    """Checking for values in Customer Gauge credentials dictionary.

    One key value is held in the Customer Gauge connector:
        - client_id: Unique cliend id number.
        - client_secret: Unique client secret.

    Args:
        BaseModel (pydantic.main.ModelMetaclass): A base class for creating
            Pydantic models.
    """

    client_id: str
    client_secret: str


class CustomerGauge(Source):
    """A class to connect and download data using Customer Gauge API.

    Documentation for each of this API's gateways:
        Responses gateway https://support.customergauge.com/support/solutions/
            articles/5000875861-get-responses
        Non-Responses gateway https://support.customergauge.com/support/solutions/
            articles/5000877703-get-non-responses
    """

    API_URL = "https://api.eu.customergauge.com/v7/rest/sync/"

    def __init__(
        self,
        *args,
        credentials: CustomerGaugeCredentials | None = None,
        config_key: str = "customer_gauge",
        **kwargs,
    ):
        """A class to connect and download data using Customer Gauge API.

        Args:
            credentials (CustomerGaugeCredentials, optional): Customer Gauge
                credentials. Defaults to None.
            config_key (str, optional): The key in the viadot config holding relevant
                credentials. Defaults to "customer_gauge".

        Raises:
            CredentialError: If credentials are not provided in local_config or
                directly as a parameter.
        """
        raw_creds = credentials or get_source_credentials(config_key)
        validated_creds = dict(CustomerGaugeCredentials(**raw_creds))

        super().__init__(*args, credentials=validated_creds, **kwargs)

        self.clean_json = None

    def _get_token(self) -> str:
        """Gets Bearer Token using POST request method.

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
            message = "The token could not be generated. Check your credentials."
            raise APIError(message)

        return token

    def _field_reference_unpacker(
        self,
        json_response: dict[str, Any],
        field: str,
    ) -> dict[str, Any]:
        """Unpack and modify dictionaries within the specified field of a JSON response.

        This function takes a JSON response and a field name. It processes dictionaries
        within the specified field, checking if each dictionary contains
        exactly two items. If a dictionary meets this criteria, it is transformed
        into a new dictionary, where the first key becomes a key, and the second key
        becomes its associated value

        Args:
            json_response (dict[str, Any], optional): JSON response with data.
            field (str): The key (column) of the dictionary to be modified.

        Returns:
            dict[str, Any]: The JSON response with modified nested dictionaries
            within the specified field.

        Raises:
            ValueError: If a dictionary within the specified field doesn't contain
                exactly two items.
        """
        result = {}
        for _, dictionary in enumerate(json_response[field]):
            if isinstance(dictionary, dict) and len(dictionary.items()) == 2:  # noqa: PLR2004
                list_properties = list(dictionary.values())
                result[list_properties[0]] = list_properties[1]
            else:
                message = (
                    "Dictionary within the specified field "
                    "doesn't contain exactly two items."
                )
                raise ValueError(message)
        if result:
            json_response[field] = result

        return json_response

    def _nested_dict_transformer(
        self,
        json_response: dict[str, Any],
        field: str,
    ) -> dict[str, Any]:
        """Modify nested dictionaries within the specified field of a JSON response.

        This function takes a JSON response and a field name. It modifies nested
        dictionaries within the specified field by adding an index and underscore
        to the keys. The modified dictionary is then updated in the JSON response.

        Args:
            json_response (dict[str, Any], optional): JSON response with data.
            field (str): The key (column) of the dictionary to be modified.

        Returns:
            dict[str, Any]: The JSON response with modified nested dictionaries
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
            self.logger.exception(type_error)  # noqa: TRY401
            raise

        return json_response

    def _get_json_response(
        self,
        url: str | None = None,
        cursor: int | None = None,
        pagesize: int = 1000,
        date_field: str | None = None,
        start_date: datetime | None = None,
        end_date: datetime | None = None,
    ) -> dict[str, Any]:
        """Get JSON that contains data and cursor parameter value.

        Args:
            url (str, optional): URL API direction. Defaults to None.
            cursor (int, optional): Cursor value to navigate to the page.
                Defaults to None.
            pagesize (int, optional): Number of responses (records) returned per page,
                max value = 1000. Defaults to 1000. Defaults to 1000.
            date_field (str, optional): Specifies the date type which filter date range.
                Possible options: "date_creation", "date_order", "date_sent" or
                "date_survey_response". Defaults to None.
            start_date (datetime, optional): Defines the period start date in
                yyyy-mm-dd format. Defaults to None.
            end_date (datetime, optional): Defines the period end date in
                yyyy-mm-dd format. Defaults to None.

        Raises:
            ValueError: If at least one date argument were provided and the rest is
                missing. Needed all 3 or skip them.
            APIError: If no response from API call.

        Returns:
            dict[str, Any]: JSON with data and cursor parameter value.
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
                message = (
                    "Missing date arguments: 'date_field', 'start_date', "
                    "'end_date'. Provide all 3 arguments or skip all of them."
                )
                raise ValueError(message)

        header = {"Authorization": f"Bearer {self._get_token()}"}
        api_response = handle_api_response(url=url, headers=header, params=params)
        response = api_response.json()

        if response is None:
            message = "No response."
            raise APIError(message)

        return response

    def _get_cursor(self, json_response: dict[str, Any] | None = None) -> int:
        """Returns cursor value that is needed to navigate to the next page.

        Args:
            json_response (dict[str, Any], optional): Dictionary with nested structure
                that contains data and cursor parameter value. Defaults to None.

        Raises:
            ValueError: If cursor value not found.

        Returns:
            int: Cursor value.
        """
        try:
            cur = json_response["cursor"]["next"]
        except KeyError as error:
            message = (
                "Provided argument doesn't contain 'cursor' value. "
                "Pass json returned from the endpoint."
            )
            raise ValueError(message) from error

        return cur

    def _get_data(
        self,
        json_response: dict[str, Any] | None = None,
    ) -> list[dict[str, Any]]:
        """Extract the 'data' part of a JSON response as a list of dictionaries.

        Args:
            json_response (Dict[str, Any], optional): JSON object represented as a
                nested dictionary that contains data and cursor parameter value.
                Defaults to None.

        Raises:
            KeyError: If the 'data' key is not present in the provided JSON response.

        Returns:
            list[dict[str, Any]]: A list of dictionaries containing data from the 'data'
                part of the JSON response.
        """
        jsons_list = []
        try:
            jsons_list = json_response["data"]
        except KeyError:
            message = (
                "Provided argument doesn't contain 'data' value. "
                "Pass json returned from the endpoint."
            )
            self.logger.exception(message)
            raise

        return jsons_list

    def _unpack_columns(
        self,
        json_list: list[dict[str, Any]] | None = None,
        unpack_by_field_reference_cols: list[str] | None = None,
        unpack_by_nested_dict_transformer: list[str] | None = None,
    ) -> list[dict[str, Any]]:
        """Function to unpack and modify specific columns in a list of dictionaries.

        It is using one of two methods, chosen by the user.
        If user would like to use field_reference_unpacker, he/she needs to provide
        list of fields as strings in `unpack_by_field_reference_cols`  parameter,
        if user would like to use nested_dict_transformer he/she needs to provide
        list of fields as strings in unpack_by_nested_dict_transformer parameter.

        Args:
            json_list (list[dict[str, Any]): A list of dictionaries containing the data.
            unpack_by_field_reference_cols (list[str]): Columns to unpack and modify
                using `_field_reference_unpacker`. Defaults to None.
            unpack_by_nested_dict_transformer (list[str]): Columns to unpack and
                modify using `_nested_dict_transformer`. Defaults to None.

        Raises:
            ValueError: If 'json_list' is not provided.
            ValueError: If specified columns do not exist in the JSON data.
            ValueError: If columns are mentioned in both
                'unpack_by_field_reference_cols' and 'unpack_by_nested_dict_transformer'

        Returns:
            list[dict[str, Any]]: The updated list of dictionaries after column
                unpacking and modification.
        """
        duplicated_cols = []

        if json_list is None:
            message = "Input 'json_list' is required."
            raise ValueError(message)

        def unpack_columns(columns: list[str], unpack_function: dict[str, Any]):
            json_list_clean = json_list.copy()
            for field in columns:
                if field in json_list_clean[0]:
                    self.logger.info(
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
                        self.logger.info(
                            f"All elements in '{field}' are unpacked successfully."
                        )
                    except ValueError:
                        self.logger.info(
                            f"No transformation were made in '{field}',"
                            "because didn't contain list of key-value data."
                        )
                else:
                    self.logger.info(f"Column '{field}' not found.")
            return json_list_clean

        if unpack_by_field_reference_cols and unpack_by_nested_dict_transformer:
            duplicated_cols = set(unpack_by_field_reference_cols).intersection(
                set(unpack_by_nested_dict_transformer)
            )
        if duplicated_cols:
            message = (
                f"{duplicated_cols} were mentioned in both"
                "unpack_by_field_reference_cols and unpack_by_nested_dict_transformer."
                "It's not possible to apply two methods to the same field."
            )
            raise ValueError(message)

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

    def api_connection(
        self,
        endpoint: Literal["responses", "non-responses"] = "non-responses",
        cursor: int | None = None,
        pagesize: int = 1000,
        date_field: str | None = None,
        start_date: datetime | None = None,
        end_date: datetime | None = None,
        total_load: bool = True,
        unpack_by_field_reference_cols: list[str] | None = None,
        unpack_by_nested_dict_transformer: list[str] | None = None,
    ) -> None:
        """General method to connect to Customer Gauge API and generate the response.

        Args:
            endpoint (Literal["responses", "non-responses"], optional): Indicate which
                endpoint to connect. Defaults to "non-responses.
            cursor (int, optional): Cursor value to navigate to the page.
                Defaults to None.
            pagesize (int, optional): Number of responses (records) returned per page,
                max value = 1000. Defaults to 1000. Defaults to 1000.
            date_field (str, optional): Specifies the date type which filter date range.
                Possible options: "date_creation", "date_order", "date_sent" or
                "date_survey_response". Defaults to None.
            start_date (datetime, optional): Defines the period start date in
                yyyy-mm-dd format. Defaults to None.
            end_date (datetime, optional): Defines the period end date in
                yyyy-mm-dd format. Defaults to None.
            total_load (bool, optional): Indicate whether to download the data to the
                latest. If 'False', only one API call is executed (up to 1000 records).
                Defaults to True.
            unpack_by_field_reference_cols (list[str]): Columns to unpack and modify
                using `_field_reference_unpacker`. Defaults to None.
            unpack_by_nested_dict_transformer (list[str]): Columns to unpack and modify
                using `_nested_dict_transformer`. Defaults to None.
        """
        url = f"{self.API_URL}{endpoint}"
        self.logger.info(f"Starting downloading data from {url} endpoint...")

        total_json = []
        json_data = self._get_json_response(
            url=url,
            cursor=cursor,
            pagesize=pagesize,
            date_field=date_field,
            start_date=start_date,
            end_date=end_date,
        )
        cur = self._get_cursor(json_data)
        jsn = self._get_data(json_data)

        if total_load:
            if cursor is None:
                self.logger.info(
                    f"Downloading all the data from the {endpoint} endpoint."
                    "Process might take a few minutes..."
                )
            else:
                self.logger.info(
                    f"Downloading starting from the {cursor} cursor. "
                    "Process might take a few minutes..."
                )
            while jsn:
                json_data = self._get_json_response(url=url, cursor=cur)
                cur = self._get_cursor(json_data)
                jsn = self._get_data(json_data)
                total_json += jsn

        self.clean_json = self._unpack_columns(
            json_list=total_json,
            unpack_by_field_reference_cols=unpack_by_field_reference_cols,
            unpack_by_nested_dict_transformer=unpack_by_nested_dict_transformer,
        )

    def _flatten_json(
        self, json_response: dict[str, Any] | None = None
    ) -> dict[str, Any]:
        """Flattens a nested structure of the JSON object.

        Function flattens it into a single-level dictionary. It uses a nested
        `flattify()` function to recursively combine nested keys in the JSON
        object with '_' to create the flattened keys.

        Args:
            json_response (dict[str, Any], optional): JSON object represented as a
                nested dictionary. Defaults to None.

        Raises:
            TypeError: If the 'json_response' not a dictionary.

        Returns:
            dict[str, Any]: The flattened dictionary.
        """
        result = {}

        if not isinstance(json_response, dict):
            message = "Input must be a dictionary."
            raise TypeError(message)

        def flattify(
            field: dict[str, Any], key: str = "", out: dict[str, Any] | None = None
        ):
            if out is None:
                out = result

            if isinstance(field, dict):
                for item in field:
                    flattify(field[item], key + item + "_", out)
            else:
                out[key[:-1]] = field

        flattify(json_response)

        return result

    def _remove_square_brackets(self, df: pd.DataFrame = None) -> pd.DataFrame:
        """Replace square brackets "[]" with an empty string in a pandas DataFrame.

        Args:
            df (pd.DataFrame, optional): Replace square brackets "[]" with an empty
                string in a pandas DataFrame. Defaults to None.

        Returns:
            pd.DataFrame: The modified DataFrame with square brackets replaced by
                an empty string.
        """
        df = df.astype(str)

        return df.map(lambda x: x.strip("[]"))

    def _clean_drivers(self, drivers: str | None = None) -> str:
        """Clean and format the 'drivers' data.

        Args:
            drivers (str, optional): Column name of the data to be cleaned.
                Defaults to None.

        Returns:
            str: A cleaned and formatted string of driver data.
        """
        return (
            drivers.replace("{", "")
            .replace("}", "")
            .replace("'", "")
            .replace("label: ", "")
        )

    @add_viadot_metadata_columns
    def to_df(
        self,
        if_empty: str = "warn",
        validate_df_dict: dict[str, Any] | None = None,
        anonymize: bool = False,
        columns_to_anonymize: list[str] | None = None,
        anonymize_method: Literal["mask", "hash"] = "mask",
        anonymize_value: str = "***",
        date_column: str | None = None,
        days: int | None = None,
    ) -> pd.DataFrame:
        """Generate a Pandas Data Frame with the data in the Response and metadata.

        Args:
            if_empty (str, optional): What to do if a fetch produce no data.
                Defaults to "warn
            validate_df_dict (dict[str, Any], optional): A dictionary with optional list
                of tests to verify the output dataframe. If defined, triggers the
                `validate_df` task from task_utils. Defaults to None.
            anonymize (bool, optional): Indicates if anonymize selected columns.
                Defaults to False.
            columns_to_anonymize (list[str], optional): List of columns to anonymize.
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

        Returns:
            pd.Dataframe: The response data as a Pandas Data Frame plus viadot metadata.
        """
        super().to_df(if_empty=if_empty)

        self.logger.info("Inserting data into the DataFrame...")

        data_frame = pd.DataFrame(list(map(self._flatten_json, self.clean_json)))
        data_frame = self._remove_square_brackets(data_frame)

        if "drivers" in list(data_frame.columns):
            data_frame["drivers"] = data_frame["drivers"].apply(self._clean_drivers)
        data_frame.columns = data_frame.columns.str.lower().str.replace(" ", "_")

        if data_frame.empty:
            self._handle_if_empty(
                if_empty=if_empty,
                message="The response does not contain any data.",
            )
        else:
            self.logger.info(
                "Successfully downloaded data from the Customer Gauge API."
            )

        if validate_df_dict is not None:
            validate(df=data_frame, tests=validate_df_dict)

        if anonymize:
            data_frame = anonymize_df(
                df=data_frame,
                columns=columns_to_anonymize,
                method=anonymize_method,
                mask_value=anonymize_value,
                date_column=date_column,
                days=days,
            )
            self.logger.info("Data Frame anonymized")

        return data_frame
