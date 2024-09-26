"""Flows for downloading data from Azure SQL and uploading it to Azure ADLS."""

from typing import Any, Dict, List, Literal

from prefect import flow
from prefect.task_runners import ConcurrentTaskRunner
from viadot.orchestration.prefect.tasks import df_to_adls, azure_sql_to_df


@flow(
    name="Azure SQL extraction to ADLS",
    description="Extract data from Azure SQL"
    + " and load it into Azure Data Lake Storage.",
    retries=1,
    retry_delay_seconds=60,
    task_runner=ConcurrentTaskRunner,
    log_prints=True,
)
def azure_sql_to_adls(
    query: str = None,
    credentials_secret: str = None,
    sep: str = "\t",
    file_path: str = None,
    if_exists: Literal["replace", "append", "delete"] = "replace",
    validate_df_dict: Dict[str, Any] = None,
    convert_bytes: bool = False,
    remove_special_characters: bool = None,
    columns_to_clean: List[str] = None,
    adls_config_key: str | None = None,
    adls_azure_key_vault_secret: str | None = None,
    adls_path: str | None = None,
    adls_path_overwrite: bool = False,
) -> None:
    r"""
    Download data from Azure SQL to a CSV file and uploading it to ADLS.

    Args:
        query (str): Query to perform on a database. Defaults to None.
        credentials_secret (str, optional): The name of the Azure Key Vault
            secret containing a dictionary with database credentials.
            Defaults to None.
        sep (str, optional): The delimiter for the output CSV file. Defaults to "\t".
        file_path (str, optional): Local destination path. Defaults to None.
        if_exists (Literal, optional): What to do if the table exists.
            Defaults to "replace".
        validate_df_dict (Dict[str], optional): A dictionary with optional list of
            tests to verify the output dataframe. If defined, triggers the `validate_df`
            task from task_utils. Defaults to None.
        convert_bytes (bool). A boolean value to trigger method df_converts_bytes_to_int
            Defaults to False.
        remove_special_characters (str, optional): Call a function that remove
            special characters like escape symbols. Defaults to None.
        columns_to_clean (List(str), optional): Select columns to clean, used with
            remove_special_characters. If None whole data frame will be processed.
            Defaults to None.
        adls_config_key (Optional[str], optional): The key in the viadot config holding
            relevant credentials. Defaults to None.
        adls_azure_key_vault_secret (Optional[str], optional): The name of the Azure Key
            Vault secret containing a dictionary with ACCOUNT_NAME and Service Principal
            credentials (TENANT_ID, CLIENT_ID, CLIENT_SECRET) for the Azure Data Lake.
            Defaults to None.
        adls_path (Optional[str], optional): Azure Data Lake destination file path (with
            file name). Defaults to None.
        adls_path_overwrite (bool, optional): Whether to overwrite the file in ADLS.
            Defaults to True.
    """
    data_frame = azure_sql_to_df(
        query=query,
        credentials_secret=credentials_secret,
        sep=sep,
        file_path=file_path,
        if_exists=if_exists,
        validate_df_dict=validate_df_dict,
        convert_bytes=convert_bytes,
        remove_special_characters=remove_special_characters,
        columns_to_clean=columns_to_clean,
    )

    return df_to_adls(
        df=data_frame,
        path=adls_path,
        credentials_secret=adls_azure_key_vault_secret,
        config_key=adls_config_key,
        overwrite=adls_path_overwrite,
    )