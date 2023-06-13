import pandas as pd

from viadot.utils import APIError, handle_api_response

from .base import Source


class Eurostat(Source):
    """
    Class for creating instance of Eurostat connector to REST API by HTTPS response (no credentials required).
    """

    def __init__(
        self,
        dataset_code: str,
        params: dict = None,
        base_url: str = "https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data/",
        *args,
        **kwargs,
    ):
        """
        It is using HTTPS REST request to pull the data.
        No API registration or API key are required.
        Data will pull based on parameters provided in dynamic part of the url.

        Example of url: https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data/TEIBS020/?format=JSON&lang=EN&indic=BS-CSMCI-BAL
        Static part: https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data
        Dynamic part: /TEIBS020/?format=JSON&lang=EN&indic=BS-CSMCI-BAL

        Please note that for one dataset there are usually multiple data regarding different subjects.
        In order to retrive data that you are interested in you have to provide parameters with codes into 'params'.

        Args:
            dataset_code (str): The code of eurostat dataset that we would like to upload.
            params (Dict[str], optional):
                A dictionary with optional URL parameters. The key represents the parameter id, while the value is the code
                for a specific parameter, for example: params = {'unit': 'EUR'} where "unit" is the parameter that you would like to set
                and "EUR" is the code of the specific parameter. You can add more than one parameter, but only one code per parameter!
                So you CAN NOT provide list of codes as in example 'params = {'unit': ['EUR', 'USD', 'PLN']}'
                These parameters are REQUIRED in most cases to pull a specific dataset from the API.
                Both parameter and code has to be provided as a string! Defaults to None.
            base_url (str): The base URL used to access the Eurostat API. This parameter specifies the root URL for all requests made to the API.
                It should not be modified unless the API changes its URL scheme.
                Defaults to "https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data/"
        Raises:
            TypeError: If self.params is different type than a dictionary.
        """

        self.dataset_code = dataset_code
        self.params = params
        if not isinstance(self.params, dict) and self.params is not None:
            raise TypeError("Params should be a dictionary.")
        self.base_url = f"{base_url}{self.dataset_code}?format=JSON&lang=EN"

        super().__init__(*args, **kwargs)

    def get_parameters_codes(self) -> dict:
        """Function for getting available parameters with codes from dataset.

        Raises:
            ValueError: If the response from the API is empty or invalid.

        Returns:
            Dict: Key is parameter and value is a list of available codes for specific parameter.
        """

        try:
            response = handle_api_response(self.base_url)
            data = response.json()
        except APIError:
            self.logger.error(
                f"Failed to fetch data for {self.dataset_code}, please check correctness of dataset code!"
            )
            raise ValueError("DataFrame is empty!")

        # getting list of available parameters
        available_params = data["id"]

        # dictionary from JSON with keys and reletad codes values
        dimension = data["dimension"]

        # Assigning list of available codes to specific parameters
        params_and_codes = {}
        for key in available_params:
            if key in dimension:
                codes = list(dimension[key]["category"]["index"].keys())
                params_and_codes[key] = codes
        return params_and_codes

    def make_params_validation(self):
        """Function for validation of given parameters in comparison
        to parameteres and their codes from JSON.

        Raises:
            ValueError: If any of the self.params keys or values is not a string or
            any of them is not available for specific dataset.
        """

        # In order to make params validation, first we need to get params_and_codes.
        key_codes = self.get_parameters_codes()

        # Validation of type of values
        for key, val in self.params.items():
            if not isinstance(key, str) or not isinstance(val, str):
                self.logger.error(
                    "You can provide only one code per one parameter as 'str' in params!\n"
                    "CORRECT: params = {'unit': 'EUR'} | INCORRECT: params = {'unit': ['EUR', 'USD', 'PLN']}"
                )
                raise ValueError("Wrong structure of params!")

        if key_codes is not None:
            # Conversion keys and values on lowwer cases by using casefold
            key_codes_after_conversion = {
                k.casefold(): [v_elem.casefold() for v_elem in v]
                for k, v in key_codes.items()
            }
            params_after_conversion = {
                k.casefold(): v.casefold() for k, v in self.params.items()
            }

            # comparing keys and values
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

            # error loggers
            if non_available_keys:
                self.logger.error(
                    f"Parameters: '{' | '.join(non_available_keys)}' are not in dataset. Please check your spelling!\n"
                    f"Possible parameters: {' | '.join(key_codes.keys())}"
                )
            if non_available_codes:
                self.logger.error(
                    f"Parameters codes: '{' | '.join(non_available_codes)}' are not available. Please check your spelling!\n"
                    f"You can find everything via link: https://ec.europa.eu/eurostat/databrowser/view/{self.dataset_code}/default/table?lang=en"
                )
            raise ValueError("DataFrame is empty!")

    def eurostat_dictionary_to_df(self, *signals: list) -> pd.DataFrame:
        """Function for creating DataFrame from JSON pulled from Eurostat.

        Returns:
            pd.DataFrame: With 4 columns: index, geo, time, indicator.
        """

        class T_SIGNAL:
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

        # get the dictionary from the inputs
        eurostat_dictionary = signals[-1]

        for signal in signals[0]:
            signal_struct = T_SIGNAL()
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

    def get_data_frame_from_response(self) -> pd.DataFrame:
        """Function responsible for getting response, creating DataFrame using method 'eurostat_dictionary_to_df'
           with validation of provided parameters and their codes if needed.

        Raises:
            APIError: If there is an error with the API request.
            ValueError: If the resulting DataFrame is empty.

        Returns:
            pd.DataFrame: Final DataFrame or raise prefect.logger.error, if issues occur.
        """

        try:
            response = handle_api_response(self.base_url, params=self.params)
            data = response.json()
            data_frame = self.eurostat_dictionary_to_df(["geo", "time"], data)

            if data_frame.empty:
                raise ValueError
        except (APIError, ValueError):
            self.make_params_validation()

        # merging data_frame with label and last updated date
        label_col = pd.Series(str(data["label"]), index=data_frame.index, name="label")
        last_updated__col = pd.Series(
            str(data["updated"]),
            index=data_frame.index,
            name="updated",
        )
        data_frame = pd.concat([data_frame, label_col, last_updated__col], axis=1)
        return data_frame
