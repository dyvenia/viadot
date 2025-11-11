"""Extract data from OneStream SQL queries and load it to Redshift Spectrum."""

from typing import Any, Literal

from prefect import flow
from prefect.logging import get_run_logger

from viadot.orchestration.prefect.tasks import (
    create_batch_list_of_custom_subst_vars,
    df_to_redshift_spectrum,
    onestream_get_agg_sql_data_to_df,
)


@flow(
    name="extract--onestream_sql_query--redshift_spectrum",
    description="Extract data from OneStream SQL queries and load it to Redshift Spectrum.",
    retries=1,
    retry_delay_seconds=60,
)
def onestream_sql_query_data_to_redshift_spectrum(  # noqa: PLR0913
    server_url: str,
    application: str,
    sql_query: str,
    to_path: str,
    schema_name: str,
    table: str,
    custom_subst_vars: dict[str, list[Any]] | None = None,
    batch_by_subst_vars: bool = False,
    db_location: str = "Application",
    results_table_name: str = "Results",
    external_db: str = "",
    api_params: dict[str, str] | None = None,
    extension: str = ".parquet",
    if_exists: Literal["overwrite", "append"] = "overwrite",
    if_empty: Literal["warn", "skip", "fail"] = "fail",
    partition_cols: list[str] | None = None,
    index: bool = False,
    compression: str | None = None,
    sep: str = ",",
    aws_config_key: str | None = None,
    credentials_secret: str | None = None,
    onestream_credentials_secret: str | None = None,
    onestream_config_key: str = "onestream",
) -> None:
    """Extract data from a OneStream SQL query and load it into AWS Redshift Spectrum.

    This function executes a SQL query in OneStream using provided parameters
    and uploads the results to AWS Redshift Spectrum.

    When custom_subst_vars are provided and batch_by_subst_vars is True, the ingestion
    process will be split into batches. Each batch represents one combination of the
    custom_subst_vars. When batch_by_subst_vars is False, all substitution variable
    combinations are processed together into a single data frame.
    Warning! Processing custom substitution vars without batching might lead to
    out-of-memory errors when data size is too big.

    Args:
        server_url (str): OneStream server URL.
        application (str): OneStream application name.
        sql_query (str): SQL query to execute in OneStream.
        to_path (str): Path to an S3 folder where the table will be located.
        schema_name (str): AWS Glue catalog database name.
        table (str): AWS Glue catalog table name.
        custom_subst_vars (dict[str, list[Any]], optional): A dictionary mapping
            substitution variable names to lists of possible values.
            Values can be of any type that can be converted to strings, as they
            are used as substitution variables in the SQL query. Defaults to None.
        batch_by_subst_vars (bool): Whether to process data in batches based on
            substitution variable combinations. When True and custom_subst_vars is
            provided, each combination of substitution variables will be processed
            as a separate batch, creating individual parquet files in S3. When False,
            all substitution variable combinations are processed together in a single
            operation. Defaults to False.
        db_location (str): Database location path. Defaults to "Application".
        results_table_name (str): Results table name. Defaults to "Results".
        external_db (str): External database name. Defaults to "".
        api_params (dict[str, str], optional): API parameters. Defaults to None.
        extension (str): Required file type. Accepted formats: 'csv', 'parquet'.
            Defaults to ".parquet".
        if_exists (str): Whether to 'overwrite' or 'append' to existing table.
            Defaults to "overwrite".
        if_empty (Literal["warn", "skip", "fail"], optional): What to do if the
            SQL query returns no data. Defaults to "fail".
        partition_cols (list[str], optional): Columns used to create partitions.
            Only applies when dataset=True. Defaults to None.
        index (bool): Write row names (index). Defaults to False.
        compression (str, optional): Compression style (None, snappy, gzip, zstd).
            Defaults to None.
        sep (str): Field delimiter for the output file. Defaults to ','.
        aws_config_key (str, optional): Key in viadot config for AWS credentials.
            Defaults to None.
        credentials_secret (str, optional): Name of Prefect secret block with AWS
            credentials. Defaults to None.
        onestream_credentials_secret (str, optional): Name of secret storing OneStream
            credentials. Defaults to None.
        onestream_config_key (str): Key in viadot config for OneStream credentials.
            Defaults to "onestream".
    """
    logger = get_run_logger()

    if batch_by_subst_vars and not custom_subst_vars:
        msg = (
            "Invalid parameter combination: batch_by_subst_vars=True requires "
            "custom_subst_vars to be provided. Either set batch_by_subst_vars=False "
            "or provide a custom_subst_vars dictionary."
        )
        logger.error(msg)
        raise ValueError(msg)

    logger.info(
        f"Starting OneStream SQL query execution from db_location: {db_location}."
    )

    if custom_subst_vars and batch_by_subst_vars:
        # Process data in batches - each substitution variable combination separately
        custom_subst_vars_batch_list = create_batch_list_of_custom_subst_vars(
            custom_subst_vars
        )
        logger.info(
            f"Processing {len(custom_subst_vars_batch_list)} batches based on substitution variable combinations."
        )

        for i, custom_subst_var in enumerate(custom_subst_vars_batch_list, 1):
            logger.info(
                f"Processing batch {i}/{len(custom_subst_vars_batch_list)}: {custom_subst_var}."
            )
            df = onestream_get_agg_sql_data_to_df(
                server_url=server_url,
                application=application,
                sql_query=sql_query,
                custom_subst_vars=custom_subst_var,
                db_location=db_location,
                results_table_name=results_table_name,
                external_db=external_db,
                api_params=api_params,
                if_empty=if_empty,
                credentials_secret=onestream_credentials_secret,
                config_key=onestream_config_key,
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
                config_key=aws_config_key,
                credentials_secret=credentials_secret,
            )
            if_exists = "append"  # Changed to "append" to add batches to the table.
            logger.info(
                f"Batch {i}/{len(custom_subst_vars_batch_list)} completed successfully."
            )

        logger.info("All batches processed successfully.")
    else:
        # Process all data together - either no custom_subst_vars or batching disabled
        df = onestream_get_agg_sql_data_to_df(
            server_url=server_url,
            application=application,
            sql_query=sql_query,
            custom_subst_vars=custom_subst_vars,
            db_location=db_location,
            results_table_name=results_table_name,
            external_db=external_db,
            api_params=api_params,
            if_empty=if_empty,
            credentials_secret=onestream_credentials_secret,
            config_key=onestream_config_key,
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
            config_key=aws_config_key,
            credentials_secret=credentials_secret,
        )
        logger.info("Data processing completed successfully.")
