"""Eurostat API connector."""

import pandas as pd

from viadot.utils import (
    APIError,
    add_viadot_metadata_columns,
    filter_df_columns,
    handle_api_response,
    validate,
)

from .base import Source


class Eurostat(Source):
    """Eurostat REST API v1 connector.

    This module provides functionalities for connecting to Eurostat  API and download
    the datasets. It includes the following features:
    - Pulling json file with all data from specific dataset.
    - Creating pandas Data Frame from pulled json file.
    - Creating dataset parameters validation if specified.

    Typical usage example:

        eurostat = Eurostat()

        eurostat.to_df(
            dataset_code: str,
            params: dict = None,
            columns: list = None,
            tests: dict = None,
        )
    """

    base_url = "https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data/"

    def __init__(
        self,
        *args,
        dataset_code: str,
        params: dict[str, str] | None = None,
        columns: list[str] | None = None,
        tests: dict | None = None,
        **kwargs,
    ):
        """Initialize the class with Eurostat API data fetching setup.

        This method uses an HTTPS request to pull data from the Eurostat API.
        No API registration or API key is required. Data is fetched based on the
        parameters provided in the dynamic part of the URL.

        Example URL:
        Static part: https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data
        Dynamic part: /TEIBS020/?format=JSON&lang=EN&indic=BS-CSMCI-BAL

        To retrieve specific data, parameters with codes must be provided in `params`.

        Args:
            dataset_code (str):
                The code of the Eurostat dataset to be downloaded.
            params (dict[str, str] | None, optional):
                A dictionary with optional URL parameters. Each key is a parameter ID,
                and the value is a specific parameter code, e.g.,
                `params = {'unit': 'EUR'}` where "unit" is the parameter, and "EUR" is
                the code. Only one code per parameter is allowed. Defaults to None.
            columns (list[str] | None, optional):
                A list of column names (as strings) that are required from the dataset.
                Filters the data to only include the specified columns.
                Defaults to None.
            tests (dict | None, optional):
                A dictionary containing test cases for the data, including:
                - `column_size`: dict{column: size}
                - `column_unique_values`: list[columns]
                - `column_list_to_match`: list[columns]
                - `dataset_row_count`: dict{'min': number, 'max': number}
                - `column_match_regex`: dict{column: 'regex'}
                - `column_sum`: dict{column: {'min': number, 'max': number}}.
                Defaults to None.
            **kwargs:
                Additional arguments passed to the class initializer.
        """
        self.dataset_code = dataset_code
        self.params = params
        self.columns = columns
        self.tests = tests

        super().__init__(*args, **kwargs)

    def get_parameters_codes(self, url: str) -> dict[str, list[str]]:
        """Retrieve available API request parameters and their codes.

        Args:
            url (str): URL built for API call.

        Raises:
            ValueError: If the response from the API is empty or invalid.

        Returns:
            dict[str, list[str]]: Key is a parameter, and value is a list of
                available codes for the specified parameter.
        """
        response = handle_api_response(url)
        data = response.json()

        available_params = data["id"]

        # Dictionary from JSON with keys and related codes values
        dimension = data["dimension"]

        # Assigning list of available codes to specific parameters
        params_and_codes = {}
        for key in available_params:
            if key in dimension:
                codes = list(dimension[key]["category"]["index"].keys())
                params_and_codes[key] = codes
        return params_and_codes

    def validate_params(
        self, dataset_code: str, url: str, params: dict[str, str]
    ) -> None:
        """Validates given parameters against the available parameters in the dataset.

        Important:
            Each dataset in Eurostat has specific parameters that could be used for
            filtering the data. For example dataset ILC_DI04 -Mean and median income by
            household type - EU-SILC and ECHP surveys has parameter such as:
            hhhtyp (Type of household), which can be filtered by specific available
            code of this parameter, such as: 'total', 'a1' (single person),
            'a1_dhc' (single person with dependent children). Please note that each
            dataset has different parameters and different codes

        Raises:
            ValueError: If any of the `params` keys or values is not a string or
            any of them is not available for specific dataset.
        """
        # In order to make params validation, first we need to get params_and_codes.
        key_codes = self.get_parameters_codes(url=url)

        if key_codes is not None:
            # Conversion keys and values on lower cases by using casefold
            key_codes_after_conversion = {
                k.casefold(): [v_elem.casefold() for v_elem in v]
                for k, v in key_codes.items()
            }
            params_after_conversion = {
                k.casefold(): v.casefold() for k, v in params.items()
            }

            # Comparing keys and values
            non_available_keys = [
                key
                for key in params_after_conversion
                if key not in key_codes_after_conversion
            ]
            non_available_codes = [
                value
                for key, value in params_after_conversion.items()
                if key in key_codes_after_conversion
                and value not in key_codes_after_conversion[key]
            ]

            # Error loggers

            if non_available_keys:
                self.logger.error(
                    f"Parameters: '{' | '.join(non_available_keys)}' are not in "
                    "dataset. Please check your spelling!"
                )

                self.logger.error(
                    f"Possible parameters: {' | '.join(key_codes.keys())}"
                )
            if non_available_codes:
                self.logger.error(
                    f"Parameters codes: '{' | '.join(non_available_codes)}' are not "
                    "available. Please check your spelling!\n"
                )
                self.logger.error(
                    "You can find everything via link: https://ec.europa.eu/"
                    f"eurostat/databrowser/view/{dataset_code}/default/table?lang=en"
                )

            if non_available_keys or non_available_codes:
                msg = "Wrong parameters or codes were provided!"
                raise ValueError(msg)

    def eurostat_dictionary_to_df(self, *signals: list[str]) -> pd.DataFrame:
        """Function for creating DataFrame from JSON pulled from Eurostat.

        Returns:
            pd.DataFrame: Pandas DataFrame With 4 columns: index, geo, time, indicator
        """

        class TSignal:
            """Class representing a signal with keys, indexes, labels, and name.

            Attributes:
            signal_keys_list : list[str]
                A list of keys representing unique identifiers for the signal.
            signal_index_list : list[str]
                A list of index values corresponding to the keys of the signal.
            signal_label_list : list[str]
                A list of labels providing human-readable names for the
                signal's keys.
            signal_name : str
                The name of the signal.
            """

            signal_keys_list: list[str]
            signal_index_list: list[str]
            signal_label_list: list[str]
            signal_name: str

        # Dataframe creation
        columns0 = signals[0].copy()
        columns0.append("indicator")
        df = pd.DataFrame(columns=columns0)
        indicator_list = []
        index_list = []
        signal_lists = []

        # Get the dictionary from the inputs
        eurostat_dictionary = signals[-1]

        for signal in signals[0]:
            signal_struct = TSignal()
            signal_struct.signal_name = signal
            signal_struct.signal_keys_list = list(
                eurostat_dictionary["dimension"][signal]["category"]["index"].keys()
            )
            signal_struct.signal_index_list = list(
                eurostat_dictionary["dimension"][signal]["category"]["index"].values()
            )
            signal_label_dict = eurostat_dictionary["dimension"][signal]["category"][
                "label"
            ]
            signal_struct.signal_label_list = [
                signal_label_dict[i] for i in signal_struct.signal_keys_list
            ]
            signal_lists.append(signal_struct)

        col_signal_temp = []
        row_signal_temp = []
        for row_index, row_label in zip(
            signal_lists[0].signal_index_list,
            signal_lists[0].signal_label_list,
            strict=False,
        ):  # rows
            for col_index, col_label in zip(
                signal_lists[1].signal_index_list,
                signal_lists[1].signal_label_list,
                strict=False,
            ):  # cols
                index = str(
                    col_index + row_index * len(signal_lists[1].signal_label_list)
                )
                if index in eurostat_dictionary["value"]:
                    index_list.append(index)
                    col_signal_temp.append(col_label)
                    row_signal_temp.append(row_label)

        indicator_list = [eurostat_dictionary["value"][i] for i in index_list]
        df.indicator = indicator_list
        df[signal_lists[1].signal_name] = col_signal_temp
        df[signal_lists[0].signal_name] = row_signal_temp

        return df

    @add_viadot_metadata_columns
    def to_df(self) -> pd.DataFrame:
        """Function responsible for getting response and creating DataFrame.

        It is using method 'eurostat_dictionary_to_df' with validation
        of provided parameters and their codes if needed.

        Raises:
            TypeError: If self.params is different type than a dictionary.
            TypeError: If self.columns is different type than a list containing strings.
            APIError: If there is an error with the API request.

        Returns:
            pd.DataFrame: Pandas DataFrame.
        """
        # Checking if params and columns have correct type
        if not isinstance(self.params, dict) and self.params is not None:
            msg = "Params should be a dictionary."
            raise TypeError(msg)

        if not isinstance(self.columns, list) and self.columns is not None:
            msg = "Requested columns should be provided as list of strings."
            raise TypeError(msg)

        # Creating url for connection with API
        url = f"{self.base_url}{self.dataset_code}?format=JSON&lang=EN"

        # Making parameters validation
        if self.params is not None:
            self.validate_params(
                dataset_code=self.dataset_code, url=url, params=self.params
            )

        # Getting response from API
        try:
            response = handle_api_response(url, params=self.params)
        except APIError:
            self.validate_params(
                dataset_code=self.dataset_code, url=url, params=self.params
            )
        data = response.json()
        data_frame = self.eurostat_dictionary_to_df(["geo", "time"], data)

        # Merge data_frame with label and last updated date
        label_col = pd.Series(str(data["label"]), index=data_frame.index, name="label")
        last_updated__col = pd.Series(
            str(data["updated"]),
            index=data_frame.index,
            name="updated",
        )
        data_frame = pd.concat([data_frame, label_col, last_updated__col], axis=1)

        # Validation and transformation of requested column
        if self.columns is not None:
            filter_df_columns(data_frame=data_frame, columns=self.columns)

        # Additional validation from utils
        validate(df=data_frame, tests=self.tests)

        return data_frame
