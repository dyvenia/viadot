"""Extract data from Salesforce API and load it into AWS Redshift Spectrum."""

from typing import Literal

from prefect import flow

from viadot.orchestration.prefect.tasks import (
    df_to_redshift_spectrum,
    salesforce_to_df,
)
from viadot.orchestration.prefect.utils import (
    with_flow_timeout_param,
    with_state_tracking_and_downstream_triggering,
)


@flow(
    name="Salesforce extraction to redshift spectrum",
    retries=1,
    retry_delay_seconds=60,
)
@with_flow_timeout_param()
@with_state_tracking_and_downstream_triggering(node_name_param="table")
def salesforce_to_redshift_spectrum(  # noqa: PLR0913
    to_path: str,
    schema_name: str,
    table: str,
    extension: str = ".parquet",
    if_exists: Literal["overwrite", "append"] = "overwrite",
    partition_cols: list[str] | None = None,
    compression: Literal["snappy", "gzip", "zstd"] | None = None,
    sep: str = ",",
    aws_config_key: str | None = None,
    credentials_secret: str | None = None,
    salesforce_credentials_secret: str | None = None,
    env: str | None = None,
    domain: str | None = None,
    query: str | None = None,
    salesforce_table: str | None = None,
    columns: list[str] | None = None,
    chunk_size: int | None = None,
) -> None:
    """Extract data from Salesforce API and load it into AWS Redshift Spectrum.

    Args:
        to_path (str): The destination path in the AWS Redshift Spectrum
            storage (S3) where the data will be saved.
        schema_name (str): The name of the schema under which the table
            will be created or updated.
        table (str): The name of the target table in AWS Redshift Spectrum.
        extension (str, optional): The file extension to use for the
            output files. Defaults to ".parquet".
        if_exists (Literal["overwrite", "append"], optional): What to do
            if the table already exists. Defaults to "overwrite".
        partition_cols (list[str], optional): The columns to partition the
            data by when writing to storage. Defaults to None.
        compression (Literal["snappy", "gzip", "zstd"], optional): The
            compression algorithm to use for the output files. Defaults to
            None.
        sep (str, optional): The separator to use, if applicable to the
            chosen `extension`. Defaults to ",".
        aws_config_key (str, optional): The key in the viadot config
            holding AWS credentials. Defaults to None.
        credentials_secret (str, optional): The name of the secret storing
            AWS credentials, alternative to `aws_config_key`. Defaults to
            None.
        salesforce_credentials_secret (str, optional): The name of the
            secret storing Salesforce credentials. Defaults to None.
        env (str, optional): The environment to connect to, e.g. "prod" or
            "sandbox". Defaults to None.
        domain (str, optional): The Salesforce domain to use when
            authenticating. Defaults to None.
        client_id (str, optional): The client ID to use for the connection.
            Defaults to None.
        query (str, optional): A custom SOQL query to use for extracting
            data from Salesforce. Defaults to None.
        salesforce_table (str, optional): The name of the Salesforce
            table to extract data from. Defaults to None.
        columns (list[str], optional): The list of columns to extract from
            the Salesforce table. Defaults to None.
        chunk_size (int, optional): The number of rows to be fetched in each chunk.
            Defaults to None.
    """
    chunks = salesforce_to_df(
        salesforce_credentials_secret=salesforce_credentials_secret,
        env=env,
        domain=domain,
        query=query,
        table=salesforce_table,
        columns=columns,
        chunk_size=chunk_size,
    )
    for i, chunk_df in enumerate(chunks):
        df_to_redshift_spectrum(
            df=chunk_df,
            to_path=to_path,
            schema_name=schema_name,
            table=table,
            extension=extension,
            if_exists=if_exists if i == 0 else "append",
            partition_cols=partition_cols,
            compression=compression,
            sep=sep,
            config_key=aws_config_key,
            credentials_secret=credentials_secret,
        )
