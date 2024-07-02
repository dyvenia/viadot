"""Flows for downloading data from Sharepoint and uploading it to AWS Redshift Spectrum."""  # noqa: W505

from pathlib import Path
from typing import Any, Literal

import pandas as pd
from prefect import flow

from viadot.orchestration.prefect.tasks import (
    df_to_redshift_spectrum,
    get_endpoint_type_from_url,
    scan_sharepoint_folder,
    sharepoint_to_df,
    validate_and_reorder_dfs_columns,
)


def load_data_from_sharepoint(
    file_sheet_mapping: dict | None,
    download_all_files: bool,
    sharepoint_url: str,
    sheet_name: str | list[str | int] | int | None = None,
    columns: str | list[str] | list[int] | None = None,
    na_values: list[str] | None = None,
    credentials_config: dict[str, Any] | None = None,
) -> dict:
    """Loads data from SharePoint and returns it as a dictionary of DataFrames.

    This function fetches data from SharePoint based on the provided file-sheet
    mapping or by downloading all files in a specified folder. It returns the
    data as a dictionary where keys are filenames and values are the respective
    DataFrames.

    Args:
        file_sheet_mapping (dict): A dictionary where keys are filenames and values are
            the sheet names to be loaded from each file. If provided, only these files
            and sheets will be downloaded.
        download_all_files (bool): A flag indicating whether to download all files from
            the SharePoint folder specified by the `sharepoint_url`. This is ignored if
            `file_sheet_mapping` is provided.
        sharepoint_url (str): The base URL of the SharePoint site or folder.
        sheet_name (str): The name of the sheet to load if `file_sheet_mapping` is not
            provided. This is used when downloading all files.
        columns (str | list[str] | list[int], optional): Which columns to ingest.
            Defaults to None.
        na_values (list[str] | None): Additional strings to recognize as NA/NaN.
            If list passed, the specific NA values for each column will be recognized.
            Defaults to None.
        credentials_config (dict, optional): A dictionary containing credentials and
            configuration for SharePoint. Defaults to None.

    Returns:
        dict: A dictionary where keys are filenames and values are DataFrames containing
            the data from the corresponding sheets.

    Options:
        - If `file_sheet_mapping` is provided, only the specified files and sheets are
        downloaded.
        - If `download_all_files` is True, all files in the specified SharePoint folder
        are downloaded and the data is loaded from the specified `sheet_name`.
    """
    dataframes_dict = {}
    credentials_secret = (
        credentials_config.get("secret") if credentials_config else None
    )
    credentials = credentials_config.get("credentials") if credentials_config else None
    config_key = credentials_config.get("config_key") if credentials_config else None

    if file_sheet_mapping:
        for file, sheet in file_sheet_mapping.items():
            df = sharepoint_to_df(
                url=sharepoint_url + file,
                sheet_name=sheet,
                columns=columns,
                na_values=na_values,
                credentials_secret=credentials_secret,
                credentials=credentials,
                config_key=config_key,
            )
            dataframes_dict[file] = df
    elif download_all_files:
        list_of_urls = scan_sharepoint_folder(
            url=sharepoint_url,
            credentials_secret=credentials_secret,
            credentials=credentials,
            config_key=config_key,
        )
        for file_url in list_of_urls:
            df = sharepoint_to_df(
                url=file_url,
                sheet_name=sheet_name,
                columns=columns,
                na_values=na_values,
                credentials_secret=credentials_secret,
                credentials=credentials,
                config_key=config_key,
            )
            file_name = Path(file_url).stem + Path(file_url).suffix
            dataframes_dict[file_name] = df
    return dataframes_dict


@flow(
    name="extract--sharepoint--redshift_spectrum",
    description="Extract data from Sharepoint and load it into AWS Redshift Spectrum.",
    retries=1,
    retry_delay_seconds=60,
)
def sharepoint_to_redshift_spectrum(  # noqa: PLR0913, PLR0917
    sharepoint_url: str,
    to_path: str,
    schema_name: str,
    table: str,
    extension: str = ".parquet",
    if_exists: Literal["overwrite", "append"] = "overwrite",
    partition_cols: list[str] | None = None,
    index: bool = False,
    compression: str | None = None,
    sep: str = ",",
    description: str | None = None,
    aws_credentials: dict[str, Any] | None = None,
    aws_config_key: str | None = None,
    sheet_name: str | list[str | int] | int | None = None,
    columns: str | list[str] | list[int] | None = None,
    na_values: list[str] | None = None,
    sharepoint_credentials_secret: str | None = None,
    sharepoint_config_key: str | None = None,
    sharepoint_credentials: dict[str, Any] | None = None,
    file_sheet_mapping: dict | None = None,
    download_all_files: bool = False,
    return_as_one_table: bool = False,
) -> None:
    """Extract data from SharePoint and load it into AWS Redshift Spectrum.

    This function downloads data from SharePoint and uploads it to AWS Redshift
    Spectrum, either as a single table or as multiple tables depending on the provided
    parameters.

    Args:
        sharepoint_url (str): The URL to the file.
        to_path (str): Path to a S3 folder where the table will be located. Defaults to
            None.
        schema_name (str): AWS Glue catalog database name.
        table (str): AWS Glue catalog table name.
        partition_cols (list[str]): List of column names that will be used to create
            partitions. Only takes effect if dataset=True.
        extension (str): Required file type. Accepted file formats are 'csv' and
            'parquet'.
        if_exists (str, optional): 'overwrite' to recreate any possible existing table
            or 'append' to keep any possible existing table. Defaults to overwrite.
        partition_cols (list[str], optional): List of column names that will be used to
            create partitions. Only takes effect if dataset=True. Defaults to None.
        index (bool, optional): Write row names (index). Defaults to False.
        compression (str, optional): Compression style (None, snappy, gzip, zstd).
        sep (str, optional): Field delimiter for the output file. Defaults to ','.
        description (str, optional): AWS Glue catalog table description. Defaults to
            None.
        aws_credentials (dict[str, Any], optional): Credentials to the AWS Redshift
            Spectrum. Defaults to None.
        aws_config_key (str, optional): The key in the viadot config holding relevant
            credentials. Defaults to None.
        sheet_name (str | list | int, optional): Strings are used for sheet names.
            Integers are used in zero-indexed sheet positions (chart sheets do not count
            as a sheet position). Lists of strings/integers are used to request multiple
            sheets. Specify None to get all worksheets. Defaults to None.
        columns (str | list[str] | list[int], optional): Which columns to ingest.
            Defaults to None.
        na_values (list[str] | None): Additional strings to recognize as NA/NaN.
            If list passed, the specific NA values for each column will be recognized.
            Defaults to None.
        sharepoint_credentials_secret (str, optional): The name of the secret storing
            Sharepoint credentials. Defaults to None.
            More info on: https://docs.prefect.io/concepts/blocks/
        sharepoint_config_key (str, optional): The key in the viadot config holding
            relevant credentials. Defaults to None.
        sharepoint_credentials (dict, optional): Credentials to Sharepoint. Defaults to
            None.
        file_sheet_mapping(dict, optional): Dictionary with mapping sheet for each file
            that should be downloaded. If the parameter is provided only data from this
            dictionary are downloaded. Defaults to empty dictionary.
        download_all_files (bool, optional): Whether to download all files from
            the folder. Defaults to False.
        return_as_one_table (bool, optional): Whether to load data to a single table.
            Defaults to False.

    The function operates in two main modes:
    1. If `file_sheet_mapping` is provided, it downloads and processes only
        the specified files and sheets.
    2. If `download_all_files` is True, it scans the SharePoint folder for all files
        and processes them.

    Additionally, depending on the value of `return_as_one_table`, the data is either
        combined into a single table or uploaded as multiple tables.

    """
    sharepoint_credentials_config = {
        "secret": sharepoint_credentials_secret,
        "credentials": sharepoint_credentials,
        "config_key": sharepoint_config_key,
    }
    endpoint_type = get_endpoint_type_from_url(url=sharepoint_url)

    if endpoint_type == "file":
        df = sharepoint_to_df(
            url=sharepoint_url,
            sheet_name=sheet_name,
            columns=columns,
            na_values=na_values,
            credentials_secret=sharepoint_credentials_secret,
            config_key=sharepoint_config_key,
            credentials=sharepoint_credentials,
        )

        df_to_redshift_spectrum(
            df=df,
            to_path=to_path,
            schema_name=schema_name,
            table=table,
            extension=extension,
            if_exists=if_exists,
            partition_cols=partition_cols,
            index=index,
            compression=compression,
            sep=sep,
            description=description,
            credentials=aws_credentials,
            config_key=aws_config_key,
        )
    else:
        dataframes_dict = load_data_from_sharepoint(
            file_sheet_mapping=file_sheet_mapping,
            download_all_files=download_all_files,
            sharepoint_url=sharepoint_url,
            sheet_name=sheet_name,
            columns=columns,
            na_values=na_values,
            credentials_config=sharepoint_credentials_config,
        )

        if return_as_one_table is True:
            dataframes_list = list(dataframes_dict.values())
            validated_and_reordered_dfs = validate_and_reorder_dfs_columns(
                dataframes_list=dataframes_list
            )
            final_df = pd.concat(validated_and_reordered_dfs, ignore_index=True)

            df_to_redshift_spectrum(
                df=final_df,
                to_path=to_path,
                schema_name=schema_name,
                table=table,
                extension=extension,
                if_exists=if_exists,
                partition_cols=partition_cols,
                index=index,
                compression=compression,
                sep=sep,
                description=description,
                credentials=aws_credentials,
                config_key=aws_config_key,
            )

        elif return_as_one_table is False:
            for file_name, df in dataframes_dict.items():
                file_name_without_extension = Path(file_name).stem
                df_to_redshift_spectrum(
                    df=df,
                    to_path=f"{to_path}_{file_name_without_extension}",  # to test
                    schema_name=schema_name,
                    table=f"{table}_{file_name_without_extension}",
                    extension=extension,
                    if_exists=if_exists,
                    partition_cols=partition_cols,
                    index=index,
                    compression=compression,
                    sep=sep,
                    description=description,
                    credentials=aws_credentials,
                    config_key=aws_config_key,
                )
