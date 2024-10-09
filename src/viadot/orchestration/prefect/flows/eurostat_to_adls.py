"""Download data from Eurostat and upload it to Azure Data Lake Storage."""

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
    adls_path: str,
    params: dict[str, str] | None = None,
    columns: list[str] | None = None,
    tests: dict | None = None,
    adls_credentials_secret: str | None = None,
    overwrite_adls: bool = False,
    adls_config_key: str | None = None,
) -> None:
    """Flow for downloading data from Eurostat to Azure Data Lake.

    This module provides a prefect flow function to use the Eurostat connector:
    - Call the prefect task wrapper to retrieve a DataFrame from the connector.
    - Upload the retrieved data to Azure Data Lake Storage.

    Args:
        dataset_code (str):
            The code of the Eurostat dataset to be uploaded.
        adls_path (str | None, optional): The destination folder or path
            in Azure Data Lake.
        params (dict[str, str] | None, optional):
            A dictionary with optional URL parameters. Each key is a parameter ID,
            and the value is a specific parameter code, e.g.,
            `params = {'unit': 'EUR'}` where "unit" is the parameter, and "EUR"
            is the code. You can pass one code per parameter. Defaults to None.
        columns (list[str] | None, optional):
            A list of columns to filter from the DataFrame after downloading.
            This acts as a filter to retrieve only the needed columns. Defaults to None.
        tests (dict | None, optional):
            A dictionary containing test cases for the data, such as:
            - `column_size`: dict{column: size}
            - `column_unique_values`: list[columns]
            - `column_list_to_match`: list[columns]
            - `dataset_row_count`: dict{'min': number, 'max': number}
            - `column_match_regex`: dict{column: 'regex'}
            - `column_sum`: dict{column: {'min': number, 'max': number}}.
            Defaults to None.

        adls_credentials_secret (str | None, optional):
            The Azure Key Vault secret containing Service Principal credentials
            (TENANT_ID, CLIENT_ID, CLIENT_SECRET) and ACCOUNT_NAME for Azure Data
            Lake access. Defaults to None.
        overwrite_adls (bool, optional):
            Whether to overwrite files in the lake if they exist. Defaults to False.
        adls_config_key (str | None, optional):
            The key in the viadot config that holds the credentials for Azure
            Data Lake. Defaults to None.

    Returns:
        None
    """
    df = eurostat_to_df(
        dataset_code=dataset_code,
        params=params,
        columns=columns,
        tests=tests,
    )
    df_to_adls(
        df=df,
        path=adls_path,
        credentials_secret=adls_credentials_secret,
        config_key=adls_config_key,
        overwrite=overwrite_adls,
    )
