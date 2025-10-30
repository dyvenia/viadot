"""Extract data from OneStream Data Adapters and load it to Redshift Spectrum."""

from typing import Any, Literal

from prefect import flow

from viadot.orchestration.prefect.tasks import (
    df_to_redshift_spectrum,
)
from viadot.orchestration.prefect.tasks.onestream import (
    onestream_get_agg_adapter_endpoint_data_to_df,
)


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
    custom_vars: dict[str, Any] | None = None,
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
        custom_vars (dict[str, Any] | None): Variables for combinations.
            Defaults to None.
        api_params (dict[str, str] | None): API parameters. Defaults to None.
        extension (str): Required file type. Accepted formats: 'csv', 'parquet'.
            Defaults to ".parquet".
        if_exists (str): Whether to 'overwrite' or 'append' to existing table.
            Defaults to "overwrite".
        partition_cols (list[str] | None): Columns used to create partitions.
            Only applies when dataset=True. Defaults to None.
        index (bool): Write row names (index). Defaults to False.
        compression (str | None): Compression style (None, snappy, gzip, zstd).
            Defaults to None.
        sep (str): Field delimiter for the output file. Defaults to ','.
        aws_config_key (str | None): Key in viadot config for AWS credentials.
            Defaults to None.
        credentials_secret (str | None): Name of Prefect secret block with AWS
            credentials. Defaults to None.
        onestream_credentials_secret (str | None): Name of secret storing OneStream
            credentials. Defaults to None.
        onestream_config_key (str): Key in viadot config for OneStream credentials.
            Defaults to "onestream".
    """
    df = onestream_get_agg_adapter_endpoint_data_to_df(
        server_url=server_url,
        application=application,
        adapter_name=adapter_name,
        workspace_name=workspace_name,
        adapter_response_key=adapter_response_key,
        custom_vars=custom_vars,
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
