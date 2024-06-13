from viadot.orchestration.prefect.tasks import df_to_adls, eurostat_to_df

from prefect import flow


@flow(
    name="extract--eurostat--adls",
    description="Flow for downloading data from the Eurostat platform via HTTPS REST API (no credentials required) to a CSV or Parquet file. Then upload it to Azure Data Lake.",  # noqa
    retries=1,
    retry_delay_seconds=60,
)
def eurostat_to_adls(
    dataset_code: str,
    params: dict = None,
    base_url: str = "https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data/",
    requested_columns: list = None,
    tests: dict = None,
    adls_path: str = None,
    adls_credentials_secret: str = None,
    overwrite_adls: bool = False,
    adls_config_key: str = None,
) -> None:
    """
    Args:
        name (str): The name of the flow.
        dataset_code(str): The code of eurostat dataset that has to be upload.
        params (Dict[str], optional):
            A dictionary with optional URL parameters. The key represents the parameter id, while the value is the code
            for a specific parameter, for example 'params = {'unit': 'EUR'}' where "unit" is the parameter that you would like to set
            and "EUR" is the code of the specific parameter. You can add more than one parameter, but only one code per parameter!
            So you CAN NOT provide list of codes as in example 'params = {'unit': ['EUR', 'USD', 'PLN']}'
            This parameter is REQUIRED in most cases to pull a specific dataset from the API.
            Both parameter and code has to provided as a string!
            Defaults to None.
        base_url (str): The base URL used to access the Eurostat API. This parameter specifies the root URL for all requests made to the API.
            It should not be modified unless the API changes its URL scheme.
            Defaults to "https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data/"
        requested_columns (List[str], optional): List of columns that are needed from DataFrame - works as filter.
            The data are downloaded from Eurostat is the same structure every time. The filter is applied after the data is fetched.
        tests:
            - `column_size`: dict{column: size}
            - `column_unique_values`: list[columns]
            - `column_list_to_match`: list[columns]
            - `dataset_row_count`: dict: {'min': number, 'max', number}
            - `column_match_regex`: dict: {column: 'regex'}
            - `column_sum`: dict: {column: {'min': number, 'max': number}}
        adls_dir_path (str, optional): Azure Data Lake destination folder/catalog path. Defaults to None.
        adls_sp_credentials_secret (str, optional): The name of the Azure Key Vault secret containing a dictionary with
            ACCOUNT_NAME and Service Principal credentials (TENANT_ID, CLIENT_ID, CLIENT_SECRET) for the Azure Data Lake.
            Defaults to None.
        overwrite_adls (bool, optional): Whether to overwrite files in the lake. Defaults to False.
        adls_config_key (str, optional): The key in the viadot config holding relevant
            credentials. Defaults to None.
    """
    df = eurostat_to_df(
        dataset_code=dataset_code,
        params=params,
        base_url=base_url,
        requested_columns=requested_columns,
        tests=tests,
    )
    adls = df_to_adls(
        df=df,
        path=adls_path,
        credentials_secret=adls_credentials_secret,
        config_key=adls_config_key,
        overwrite=overwrite_adls,
    )
    return adls
