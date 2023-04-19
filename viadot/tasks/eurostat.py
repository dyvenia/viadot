from prefect import Task
from ..sources import Eurostat
import pandas as pd


class EurostatToDF(Task):
    """Task for creating pandas data frame from Eurostat HTTPS REST API (no credentials required)
    with optional change of columns.

    Args:
        dataset_code (str): The code of eurostat dataset that we would like to upload.
        params (Dict[str], optional):
            A dictionary with optional URL parameters. The key represents the parameter id, while the value is the code
            for a specific parameter, for example 'params = {'unit': 'EUR'}' where "unit" is the parameter that you would like to set
            and "EUR" is the code of the specific parameter. You can add more than one parameter, but only one code per parameter!
            So you CAN NOT provide list of codes as in example 'params = {'unit': ['EUR', 'USD', 'PLN']}'
            This parameter is REQUIRED in most cases to pull a specific dataset from the API.
            Both parameter and code has to provided as a string!
            Defaults to None.
        needed_columns (List[str], optional): list of needed names of columns. Names should be given as str's into the list.
            Defaults to None.
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

        super().__init__(name="eurostat_to_df", *args, **kwargs)

    def run(self) -> pd.DataFrame:
        """Run function for returning unchanged DataFrame, or modify DataFrame and returning if user need specific columns.

        Returns:
            pd.DataFrame: Unchanged DataFrame or DataFrame with only choosen columns.
        """

        data_frame = Eurostat(
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
                raise ValueError("Provided columns are not available!")

            new_df = data_frame.loc[:, needed_column_after_validation]
            return new_df
