from viadot.sources import Eurostat

from prefect import task


@task(retries=3, retry_delay_seconds=10, timeout_seconds=60 * 60)
def eurostat_to_df(
    dataset_code: str,
    params: dict = None,
    base_url: str = "https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data/",
    requested_columns: list = None,
):
    """Task for creating pandas data frame from Eurostat HTTPS REST API (no credentials required).

    Args:
        dataset_code (str): The code of eurostat dataset that we would like to upload.
        params (Dict[str], optional):
            A dictionary with optional URL parameters. The key represents the parameter id, while the value is the code
            for a specific parameter, for example 'params = {'unit': 'EUR'}' where "unit" is the parameter that you would like to set
            and "EUR" is the code of the specific parameter. You can add more than one parameter, but only one code per parameter!
            So you CAN NOT provide list of codes as in example 'params = {'unit': ['EUR', 'USD', 'PLN']}'
            This parameter is REQUIRED in most cases to pull a specific dataset from the API.
            Both parameter and code has to provided as a string! Defaults to None.
        base_url (str): The base URL used to access the Eurostat API. This parameter specifies the root URL for all requests made to the API.
            It should not be modified unless the API changes its URL scheme.
            Defaults to "https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data/"
        requested_columns (List[str], optional): list of needed names of columns. Names should be given as str's into the list.
            Defaults to None.
    Returns:
        pd.DataFrame: None empty DataFrame.
    """

    data_frame = Eurostat().to_df(dataset_code, params, base_url, requested_columns)

    return data_frame
