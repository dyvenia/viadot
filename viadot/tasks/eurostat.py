from prefect import Task
from ..sources import eurostat


class EurostatToDF(Task):
    """Task for creating pandas data frame from Eurostat API with optional change of columns.

    Args:
        dataset_code (str): The code of eurostat dataset that we would like to upload - ALWAYS REQUIRED
        time (str): optional url parameter that works as filter - only one year can be given
        params (dict, optional):
            A dictionary with optional URL parameters. The key represents the parameter id, while the value is the code
            for a specific parameter, for example 'params = {'unit': 'EUR'}' where "unit" is the parameter that you would like to set
            and "EUR" is the code of the specific parameter. You can add more than one parameter, but only one code per parameter!
            So you CAN NOT provide list of codes as in example 'params = {'unit': ['EUR', 'USD', 'PLN']}'
            This parameter is REQUIRED in most cases to pull a specific dataset from the API.
            Both parameter and code has to provided as a string!
            Defaults to None.
        needed_columns (list, optional): list of needed names of columns. Names should be given as str's into the list.
    """

    def __init__(
        self,
        dataset_code: str,
        params: dict = None,
        needed_columns: list = None,
        *args,
        **kwargs,
    ):
        self.dataset_code = dataset_code
        self.params = params
        self.needed_columns = needed_columns

        super().__init__(name="EurostatToDF", *args, **kwargs)

    def run(self):
        """Run function for returning dataset if user want raw data, or modify and returning if user need some changes.

        Returns:
            pd.DataFrame: raw DataFrame or DataFrame with choosen columns.
        """
        try:
            data_frame = eurostat.Eurostat(
                self.dataset_code, self.params
            ).get_data_frame_from_response()

            if self.needed_columns is None:
                return data_frame
            else:
                columns_list = data_frame.columns.tolist()
                columns_list = [str(column).casefold() for column in columns_list]
                needed_column_after_validation = []
                non_available_columns = []

                for column in self.needed_columns:
                    # Checking if user column is in our dataframe column list
                    column = str(column).casefold()

                    if column in columns_list:
                        needed_column_after_validation.append(column)
                    else:
                        non_available_columns.append(column)

                # Error logger
                if non_available_columns:
                    self.logger.error(
                        f"Name of the columns: '{' | '.join(non_available_columns)}' are not in DataFrame. Please check spelling!\n"
                        f"Available columns: {' | '.join(columns_list)}"
                    )
                new_df = data_frame.loc[:, needed_column_after_validation]

                if new_df.empty:
                    return None
                else:
                    return new_df
        except:
            self.logger.error(
                "EurostatToDF.run() method failed. Please, check your parameters."
            )
