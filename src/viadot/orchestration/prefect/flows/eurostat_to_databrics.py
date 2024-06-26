from typing import Literal

from viadot.orchestration.prefect.tasks import df_to_databricks, eurostat_to_df

from prefect import flow


@flow(
    name="extract--eurostat--databricks",
    description="Flow for downloading data from the Eurostat platform via HTTPS REST API (no credentials required) to a CSV or Parquet file. Then upload it to Databricks.",  # noqa
    retries=1,
    retry_delay_seconds=60,
)
def eurostat_to_databricks(
    dataset_code: str,
    databricks_table: str,
    params: dict = None,
    columns: list = None,
    tests: dict = None,
    if_exists: Literal["replace", "skip", "fail"] = "fail",
    databricks_schema: str = None,
    databricks_credentials_secret: str = None,
    databricks_config_key: str = None,
) -> None:
    """
    Args:
        dataset_code(str): The code of eurostat dataset that has to be upload.
        databricks_table (str): The name of the target table.
        params (Dict[str], optional):
            A dictionary with optional URL parameters. The key represents the parameter id, while the value is the code
            for a specific parameter, for example 'params = {'unit': 'EUR'}' where "unit" is the parameter that you would like to set
            and "EUR" is the code of the specific parameter. You can add more than one parameter, but only one code per parameter!
            So you CAN NOT provide list of codes as in example 'params = {'unit': ['EUR', 'USD', 'PLN']}'
            This parameter is REQUIRED in most cases to pull a specific dataset from the API.
            Both parameter and code has to provided as a string!
            Defaults to None.
        columns (List[str], optional): List of columns that are needed from DataFrame - works as filter.
            The data are downloaded from Eurostat is the same structure every time. The filter is applied after the data is fetched.
        tests:
            - `column_size`: dict{column: size}
            - `column_unique_values`: list[columns]
            - `column_list_to_match`: list[columns]
            - `dataset_row_count`: dict: {'min': number, 'max', number}
            - `column_match_regex`: dict: {column: 'regex'}
            - `column_sum`: dict: {column: {'min': number, 'max': number}}
        if_exists (str, Optional): What to do if the table already exists.
            One of 'replace', 'skip', and 'fail'.
        databricks_schema (str, optional): The name of the target schema.
        databricks_credentials_secret (str, optional): The name of the Azure Key Vault
            secret storing relevant credentials. Defaults to None.
        databricks_config_key (str, optional): The key in the viadot config holding
            relevant credentials. Defaults to None.
    """
    df = eurostat_to_df(
        dataset_code=dataset_code,
        params=params,
        columns=columns,
        tests=tests,
    )
    databricks = df_to_databricks(
        df=df,
        schema=databricks_schema,
        table=databricks_table,
        if_exists=if_exists,
        credentials_secret=databricks_credentials_secret,
        config_key=databricks_config_key,
    )
    return databricks
