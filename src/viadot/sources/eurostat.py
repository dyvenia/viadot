"""
'eurostat.py'.

Structure for the Eurostat API connector.

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

Functions:

    get_parameters_codes(dataset_code: str, url: str): Validate available API request
        parameters and their codes.
    validate_params(dataset_code: str, url: str, params: dict): Validates given
        parameters against the available parameters in the dataset
    eurostat_dictionary_to_df(*signals: list): Function for creating DataFrame from
        JSON pulled from Eurostat
    to_df(dataset_code: str, params: dict = None, columns: list = None,
        tests: dict = None): Function responsible for getting response and creating
        DataFrame using method 'eurostat_dictionary_to_df' with validation of provided
        parameters and their codes if needed.

"""

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
    """Class for creating instance of Eurostat connector to REST API by HTTPS response.

    (no credentials required).
    """

    base_url = "https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data/"

    def __init__(
        self,
        *args,
        dataset_code: str,
        params: dict = None,
        columns: list = None,
        tests: dict = None,
        **kwargs
    ):
        """It is using HTTPS REST request to pull the data.

        No API registration or API key are required. Data will pull based on parameters
        provided in dynamic part of the url.

        Example of url: https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/
            data/TEIBS020/?format=JSON&lang=EN&indic=BS-CSMCI-BAL

        Static part: https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data
        Dynamic part: /TEIBS020/?format=JSON&lang=EN&indic=BS-CSMCI-BAL

        Please note that for one dataset there are usually multiple data regarding
        different subjects. In order to retrieve data that you are interested in you
        have to provide parameters with codes into 'params'.

        Args:
            base_url (str): The base URL used to access the Eurostat API.
                This parameter specifies the root URL for all requests made to the API.
                It should not be modified unless the API changes its URL scheme.
                Defaults to "https://ec.europa.eu/eurostat/api/dissemination/statistics
                    /1.0/data/"
        Args:
            dataset_code (str): The code of Eurostat dataset that we would like
                to upload.
            params (Dict[str], optional):
                A dictionary with optional URL parameters. The key represents the
                parameter id, while the value is the code for a specific parameter,
                for example: params = {'unit': 'EUR'} where "unit" is the parameter
                that you would like to set and "EUR" is the code of the
                specific parameter. You can add more than one parameter,
                but only one code per parameter! So you CAN NOT provide list of codes
                as in example 'params = {'unit': ['EUR', 'USD', 'PLN']}'
                These parameters are REQUIRED in most cases to pull a specific
                dataset from the API. Both parameter and code has to be provided
                as a string! Defaults to None.
            columns (List[str], optional): list of needed names of columns.
                Names should be given as str's into the list. Defaults to None.
            tests:
                - `column_size`: dict{column: size}
                - `column_unique_values`: list[columns]
                - `column_list_to_match`: list[columns]
                - `dataset_row_count`: dict: {'min': number, 'max', number}
                - `column_match_regex`: dict: {column: 'regex'}
                - `column_sum`: dict: {column: {'min': number, 'max': number}}
        """
        self.dataset_code = dataset_code
        self.params = params
        self.columns = columns
        self.tests = tests

        super().__init__(*args, **kwargs)

    def get_parameters_codes(self, dataset_code: str, url: str) -> dict:
        """Validate available API request parameters and their codes.

        Raises:
            ValueError: If the response from the API is empty or invalid.

        Returns:
            Dict: Key is parameter and value is a list of available codes for
            specific parameter.
        """
        try:
            response = handle_api_response(url)
            data = response.json()
        except APIError as api_error:
            self.logger.error(
                f"Failed to fetch data for {dataset_code}, please check correctness "
                "of dataset code!"
            )
            raise ValueError("DataFrame is empty!") from api_error

        # Getting list of available parameters
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

    def validate_params(self, dataset_code: str, url: str, params: dict):
        """Validates given parameters against the available parameters in the dataset.

        Important:
            Each dataset in eurostat has specific parameters that could be used for
            filtering the data. For example dataset ILC_DI04 -Mean and median income by
            household type - EU-SILC and ECHP surveys has parameter such as:
            hhhtyp (Type of household), which can be filtered by specific available
            code of this parameter, such as: 'total', 'a1' (single person),
            'a1_dhc' (single person with dependent children). Please note that each
            dataset has different parameters and different codes

        Raises:
            ValueError: If any of the self.params keys or values is not a string or
            any of them is not available for specific dataset.
        """
        # In order to make params validation, first we need to get params_and_codes.
        key_codes = self.get_parameters_codes(dataset_code=dataset_code, url=url)

        # Validation of type of values
        for key, val in params.items():
            if not isinstance(key, str) or not isinstance(val, str):
                self.logger.error(
                    "You can provide only one code per one parameter as 'str' "
                    "in params! "
                    "CORRECT: params = {'unit': 'EUR'} | INCORRECT: params = "
                    "{'unit': ['EUR', 'USD', 'PLN']}"
                )
                raise ValueError("Wrong structure of params!")

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
                for key in params_after_conversion.keys()
                if key not in key_codes_after_conversion
            ]
            non_available_codes = [
                value
                for key, value in params_after_conversion.items()
                if key in key_codes_after_conversion.keys()
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
                raise ValueError("Wrong parameters or codes were provided!")

    def eurostat_dictionary_to_df(self, *signals: list) -> pd.DataFrame:
        """Function for creating DataFrame from JSON pulled from Eurostat.

        Returns:
            pd.DataFrame: Pandas DataFrame With 4 columns: index, geo, time, indicator
        """

        class TSignal:
            """Class representing a signal with keys, indexes, labels, and name."""

            signal_keys_list: list
            signal_index_list: list
            signal_label_list: list
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
            signal_lists[0].signal_index_list, signal_lists[0].signal_label_list
        ):  # rows
            for col_index, col_label in zip(
                signal_lists[1].signal_index_list, signal_lists[1].signal_label_list
            ):  # cols
                index = str(
                    col_index + row_index * len(signal_lists[1].signal_label_list)
                )
                if index in eurostat_dictionary["value"].keys():
                    index_list.append(index)
                    col_signal_temp.append(col_label)
                    row_signal_temp.append(row_label)

        indicator_list = [eurostat_dictionary["value"][i] for i in index_list]
        df.indicator = indicator_list
        df[signal_lists[1].signal_name] = col_signal_temp
        df[signal_lists[0].signal_name] = row_signal_temp

        return df

    @add_viadot_metadata_columns
    def to_df(
        self,
        if_empty="warn"
    ) -> pd.DataFrame:
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
            raise TypeError("Params should be a dictionary.")

        if not isinstance(self.columns, list) and self.columns is not None:
            raise TypeError("Requested columns should be provided as list of strings.")

        # Creating url for connection with API
        url = f"{self.base_url}{self.dataset_code}?format=JSON&lang=EN"

        # Making parameters validation
        if self.params is not None:
            self.validate_params(dataset_code=self.dataset_code,
                                 url=url,
                                 params=self.params)

        # Getting response from API
        try:
            response = handle_api_response(url, params=self.params)
            data = response.json()
            data_frame = self.eurostat_dictionary_to_df(["geo", "time"], data)
        except APIError:
            self.validate_params(dataset_code=self.dataset_code,
                                 url=url,
                                 params=self.params)

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
