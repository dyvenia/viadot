"""
'eurostat_to_databrics.py'.

Prefect flow for the Eurostat Cloud API connector.

This module provides a prefect flow function to use the Eurostat connector:
- Call to the prefect task wrapper to get a final Data Frame from the connector.
- Upload that data to Databrics Storage.

Typical usage example:

    eurostat_to_databrics(
        dataset_code: str,
        params: dict = None,
        columns: list = None,
        tests: dict = None,
        adls_path: str = None,
        adls_credentials_secret: str = None,
        overwrite_adls: bool = False,
        adls_config_key: str = None,
    )

Functions:

    eurostat_to_databrics(
        dataset_code: str,
        params: dict = None,
        columns: list = None,
        tests: dict = None,
        adls_path: str = None,
        adls_credentials_secret: str = None,
        overwrite_adls: bool = False,
        adls_config_key: str = None,
    ):
        Flow to download data from Eurostat Cloud API and upload to Databrics.
"""

from typing import Literal

from prefect import flow

from viadot.orchestration.prefect.tasks import df_to_databricks, eurostat_to_df


@flow(
    name="extract--eurostat--databricks",
    description="""Flow for downloading data from the Eurostat platform via
    HTTPS REST API (no credentials required) to a CSV or Parquet file.
    Then upload it to Databricks.""",
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
    Flow for downloading data from Eurostat to Databricks.

    Args:
        dataset_code (str): The code of the Eurostat dataset to be uploaded.
        databricks_table (str): The name of the target table.
        params (Dict[str], optional):
            A dictionary with optional URL parameters. The key represents the
            parameter ID, while the value is the code for a specific parameter,
            for example 'params = {'unit': 'EUR'}' where "unit" is the parameter
            to set and "EUR" is the specific parameter code. You can add more than
            one parameter, but only one code per parameter! So you CANNOT provide
            a list of codes, e.g., 'params = {'unit': ['EUR', 'USD', 'PLN']}'.
            This parameter is REQUIRED in most cases to pull a specific dataset
            from the API. Both the parameter and code must be provided as a string!
            Defaults to None.
        columns (List[str], optional): List of needed columns from the DataFrame
            - acts as a filter. The data downloaded from Eurostat has the same
            structure every time. The filter is applied after the data is fetched.
            Defaults to None.
        tests:
            - `column_size`: dict{column: size}
            - `column_unique_values`: list[columns]
            - `column_list_to_match`: list[columns]
            - `dataset_row_count`: dict: {'min': number, 'max': number}
            - `column_match_regex`: dict: {column: 'regex'}
            - `column_sum`: dict: {column: {'min': number, 'max': number}}
        if_exists (str, optional): What to do if the table already exists.
            One of 'replace', 'skip', or 'fail'.
        databricks_schema (str, optional): The name of the target schema.
        databricks_credentials_secret (str, optional): The name of the Azure Key
            Vault secret storing relevant credentials. Defaults to None.
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
