"""Extract data from OneStream Data Adapters and load it to Redshift Spectrum."""

from itertools import product
from typing import Any, Literal

from prefect import flow

from viadot.orchestration.prefect.tasks import (
    df_to_redshift_spectrum,
    onestream_get_agg_adapter_endpoint_data_to_df,
)


def _create_batch_list_of_custom_subst_vars(
    custom_subst_vars: dict[str, list[Any]],
) -> list[dict[str, list[Any]]]:
    """Generates a list of dictionaries of all combinations of custom subst. variables.

    Each combination will be used as a separate batch when batch_by_subst_vars is True,
    allowing for individual processing and storage of parquet files in S3.

    Args:
        custom_subst_vars (dict[str, list[Any]]): A dictionary where each key
            maps to a list of possible values for that substitution variable. The
            cartesian product of these lists will be computed.

    Returns:
        list[dict[str, list[Any]]]: A list of dictionaries for substitution variables,
            each representing a unique combination of custom variable values, where each
            dictionary has the same keys as the input but with single values selected
            from the corresponding lists.
    """
    return [
        dict(
            zip(
                custom_subst_vars.keys(),
                [[value] for value in combination],
                strict=False,
            )
        )
        for combination in product(*custom_subst_vars.values())
    ]


@flow(
    name="extract--onestream_data_adapters--redshift_spectrum",
    description="Extract data from OneStream Data Adapters and load it to Redshift Spectrum.",
    retries=1,
    retry_delay_seconds=60,
)
def onestream_data_adapters_to_redshift_spectrum(  # noqa: PLR0913
    server_url: str,
    application: str,
    adapter_name: str,
    to_path: str,
    schema_name: str,
    table: str,
    workspace_name: str = "MainWorkspace",
    adapter_response_key: str = "Results",
    custom_subst_vars: dict[str, list[Any]] | None = None,
    batch_by_subst_vars: bool = False,
    api_params: dict[str, str] | None = None,
    extension: str = ".parquet",
    if_exists: Literal["overwrite", "append"] = "overwrite",
    partition_cols: list[str] | None = None,
    index: bool = False,
    compression: str | None = None,
    sep: str = ",",
    aws_config_key: str | None = None,
    credentials_secret: str | None = None,
    onestream_credentials_secret: str | None = None,
    onestream_config_key: str = "onestream",
) -> None:
    """Extract data from OneStream Data Adapter and load it into AWS Redshift Spectrum.

    This function retrieves data from a OneStream Data Adapter using provided parameters
    and uploads it to AWS Redshift Spectrum.

    When custom_subst_vars are provided and batch_by_subst_vars is True, the ingestion
    process will be split into batches. Each batch represents one combination of the
    custom_subst_vars. When batch_by_subst_vars is False, all substitution variable
    combinations are processed together in to a single data frame.
    Warning!Processing custom substition vars without batching might lead to
    out of memory errors when data size to big.


    Args:
        server_url (str): OneStream server URL.
        application (str): OneStream application name.
        adapter_name (str): Data Adapter name to query.
        to_path (str): Path to a S3 folder where the table will be located.
        schema_name (str): AWS Glue catalog database name.
        table (str): AWS Glue catalog table name.
        workspace_name (str): OneStream workspace name. Defaults to "MainWorkspace".
        adapter_response_key (str): Key in the JSON response that contains
            the adapter's returned data. Defaults to "Results".
        custom_subst_vars (dict[str, list[Any]], optional): A dictionary mapping
            substitution variable names to lists of possible values.
            Values can be of any type that can be converted to strings, as they
            are used as substitution variables in the Data Adapter. Defaults to None.
        batch_by_subst_vars (bool): Whether to process data in batches based on
            substitution variable combinations. When True and custom_subst_vars is
            provided, each combination of substitution variables will be processed
            as a separate batch, creating individual parquet files in S3. When False,
            all substitution variable combinations are processed together in a single
            operation. Defaults to False.
        api_params (dict[str, str], optional): API parameters. Defaults to None.
        extension (str): Required file type. Accepted formats: 'csv', 'parquet'.
            Defaults to ".parquet".
        if_exists (str): Whether to 'overwrite' or 'append' to existing table.
            Defaults to "overwrite".
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
    if custom_subst_vars and batch_by_subst_vars:
        # Process data in batches - each substitution variable combination separately
        custom_subst_vars_batch_list = _create_batch_list_of_custom_subst_vars(
            custom_subst_vars
        )
        for custom_subst_var in custom_subst_vars_batch_list:
            df = onestream_get_agg_adapter_endpoint_data_to_df(
                server_url=server_url,
                application=application,
                adapter_name=adapter_name,
                workspace_name=workspace_name,
                adapter_response_key=adapter_response_key,
                custom_subst_vars=custom_subst_var,
                api_params=api_params,
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
    else:
        # Process all data together - either no custom_subst_vars or batching disabled
        df = onestream_get_agg_adapter_endpoint_data_to_df(
            server_url=server_url,
            application=application,
            adapter_name=adapter_name,
            workspace_name=workspace_name,
            adapter_response_key=adapter_response_key,
            custom_subst_vars=custom_subst_vars,
            api_params=api_params,
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
