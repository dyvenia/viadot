from prefect import flow

from viadot.orchestration.prefect.tasks import df_to_adls, eurostat_to_df


@flow(
    name="extract--eurostat--adls",
    description="""Flow for downloading data from the Eurostat platform via
    HTTPS REST API (no credentials required) to a CSV or Parquet file.
    Then upload it to Azure Data Lake.""",
    retries=1,
    retry_delay_seconds=60,
)
def eurostat_to_adls(
    dataset_code: str,
    params: dict = None,
    columns: list = None,
    tests: dict = None,
    adls_path: str = None,
    adls_credentials_secret: str = None,
    overwrite_adls: bool = True,
    adls_config_key: str = None,
) -> None:
    """
    Flow for downloading data from Eurostat to Azure Data Lake.
    Args:
        name (str): The name of the flow.
        dataset_code (str): The code of the Eurostat dataset to be uploaded.
        params (Dict[str], optional):
            A dictionary with optional URL parameters. The key represents the
            parameter ID, while the value is the code for a specific parameter,
            for example 'params = {'unit': 'EUR'}' where "unit" is the parameter
            to set and "EUR" is the specific parameter code. You can add more
            than one parameter, but only one code per parameter! So you CANNOT
            provide a list of codes, e.g., 'params = {'unit': ['EUR', 'USD',
            'PLN']}'. This parameter is REQUIRED in most cases to pull a specific
            dataset from the API. Both the parameter and code must be provided
            as a string! Defaults to None.
        columns (List[str], optional): List of needed columns from the DataFrame
            - acts as a filter. The data downloaded from Eurostat has the same
            structure every time. The filter is applied after the data is
            fetched. Defaults to None.
        tests:
            - `column_size`: dict{column: size}
            - `column_unique_values`: list[columns]
            - `column_list_to_match`: list[columns]
            - `dataset_row_count`: dict: {'min': number, 'max': number}
            - `column_match_regex`: dict: {column: 'regex'}
            - `column_sum`: dict: {column: {'min': number, 'max': number}}
        adls_dir_path (str, optional): Azure Data Lake destination folder/path.
            Defaults to None.
        adls_sp_credentials_secret (str, optional): The name of the Azure Key Vault
            secret containing a dictionary with ACCOUNT_NAME and Service Principal
            credentials (TENANT_ID, CLIENT_ID, CLIENT_SECRET) for the Azure Data
            Lake. Defaults to None.
        overwrite_adls (bool, optional): Whether to overwrite files in the lake.
            Defaults to False.
        adls_config_key (str, optional): The key in the viadot config holding
            relevant credentials. Defaults to None.
    """
    df = eurostat_to_df(
        dataset_code=dataset_code,
        params=params,
        columns=columns,
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

if __name__=="__main__":
    eurostat_to_adls(
        dataset_code= "STS_COPI_Q",
        params= {"indic_bt": "CSTI"},
        adls_path= "raw/marketing/eurostat/STS_COPI_Q_construction_producer_costs.parquet",
        adls_credentials_secret= "app-azure-cr-datalakegen2-dev",
        overwrite_adls= True
    )