"""Flows for downloading data from SAP and uploading it to AWS Redshift Spectrum."""

from typing import Any, Literal

from prefect import flow

from viadot.orchestration.prefect.tasks import df_to_redshift_spectrum, sap_rfc_to_df


@flow(
    name="extract--sap--redshift_spectrum",
    description="Extract data from SAP and load it into AWS Redshift Spectrum.",
    retries=1,
    retry_delay_seconds=60,
)
def sap_to_redshift_spectrum(  # noqa: PLR0913
    to_path: str,
    schema_name: str,
    table: str,
    tests: dict[str, Any] | None = None,
    extension: str = ".parquet",
    if_exists: Literal["overwrite", "append"] = "overwrite",
    partition_cols: list[str] | None = None,
    index: bool = False,
    compression: str | None = None,
    aws_sep: str = ",",
    dynamic_date_symbols: list[str] = ["<<", ">>"],  # noqa: B006
    dynamic_date_format: str = "%Y%m%d",
    dynamic_date_timezone: str = "UTC",
    credentials_secret: str | None = None,
    aws_config_key: str | None = None,
    query: str | None = None,
    func: str | None = None,
    rfc_total_col_width_character_limit: int = 400,
    rfc_unique_id: list[str] | None = None,
    sap_credentials_secret: str | None = None,
    sap_config_key: str | None = None,
    sap_sep: str | None = "♔",
    replacement: str = "-",
) -> None:
    """Download a pandas `DataFrame` from SAP and upload it to AWS Redshift Spectrum.

    Args:
        to_path (str): Path to a S3 folder where the table will be located.
            Defaults to None.
        schema_name (str): AWS Glue catalog database name.
        table (str): AWS Glue catalog table name.
        tests (dict[str], optional): A dictionary with optional list of tests
            to verify the output dataframe. If defined, triggers the `validate`
            function from viadot.utils. Defaults to None.
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
        aws_sep (str, optional): Field delimiter for the output file. Defaults to ','.
        dynamic_date_symbols (list[str], optional): Symbols used for dynamic date
            handling. Defaults to ["<<", ">>"].
        dynamic_date_format (str, optional): Format used for dynamic date parsing.
            Defaults to "%Y%m%d".
        dynamic_date_timezone (str, optional): Timezone used for dynamic date
            processing. Defaults to "UTC".
        aws_config_key (str, optional): The key in the viadot config holding relevant
            credentials. Defaults to None.
        credentials_secret (str, optional): The name of a secret block in Prefect
            that stores AWS credentials. Defaults to None.
        query (str): The query to be executed with pyRFC.
        func (str, optional): SAP RFC function to use. Defaults to None.
        rfc_total_col_width_character_limit (int, optional): Number of characters by
            which query will be split in chunks in case of too many columns for RFC
            function. According to SAP documentation, the limit is 512 characters.
            However, we observed SAP raising an exception even on a slightly lower
            number of characters, so we add a safety margin. Defaults to 400.
        rfc_unique_id  (list[str], optional): Reference columns to merge chunks Data
            Frames. These columns must to be unique. If no columns are provided, all
                data frame columns will by concatenated. Defaults to None.
        sap_credentials_secret (str, optional): The name of the AWS secret that stores
            SAP credentials. Defaults to None.
        sap_config_key (str, optional): The key in the viadot config holding relevant
            credentials. Defaults to None.
        replacement (str, optional): In case of sep is on a columns, set up a new
            character to replace inside the string to avoid flow breakdowns.
            Defaults to "-".
        sap_sep (str, optional): The separator to use when reading query results.
            If set to None, multiple options are automatically tried.
            Defaults to ♔.


    Examples:
        sap_to_redshift_spectrum(
            ...
            rfc_unique_id=["VBELN", "LPRIO"],
            ...
        )
    """
    df = sap_rfc_to_df(
        query=query,
        tests=tests,
        func=func,
        rfc_unique_id=rfc_unique_id,
        rfc_total_col_width_character_limit=rfc_total_col_width_character_limit,
        credentials_secret=sap_credentials_secret,
        config_key=sap_config_key,
        dynamic_date_symbols=dynamic_date_symbols,
        dynamic_date_format=dynamic_date_format,
        dynamic_date_timezone=dynamic_date_timezone,
        replacement=replacement,
        sep=sap_sep,
    )

    return df_to_redshift_spectrum(
        df=df,
        to_path=to_path,
        schema_name=schema_name,
        table=table,
        extension=extension,
        if_exists=if_exists,
        partition_cols=partition_cols,
        index=index,
        compression=compression,
        sep=aws_sep,
        config_key=aws_config_key,
        credentials_secret=credentials_secret,
    )
