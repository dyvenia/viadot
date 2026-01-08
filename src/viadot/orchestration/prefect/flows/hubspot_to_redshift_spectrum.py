"""Download data from Hubspot API and load into Redshift Spectrum."""

from typing import Any, Literal

from prefect import flow
from prefect.task_runners import ConcurrentTaskRunner

from viadot.orchestration.prefect.tasks import df_to_redshift_spectrum, hubspot_to_df


@flow(
    name="Hubspot extraction to Redshift Spectrum",
    description="Extract data from Hubspot API and load into Redshift Spectrum.",
    retries=1,
    retry_delay_seconds=60,
    task_runner=ConcurrentTaskRunner,
)
def hubspot_to_redshift_spectrum(  # noqa: PLR0913
    to_path: str,
    schema_name: str,
    table: str,
    hubspot_url: str | None = None,
    api_method: str | None = None,
    contact_type: str = "influencedContacts",
    campaign_ids: list[str] | None = None,
    filters: list[dict[str, Any]] | None = None,
    properties: list[Any] | None = None,
    nrows: int = 1000,
    hubspot_config_key: str | None = None,
    hubspot_credentials_secret: str | None = None,
    extension: str = ".parquet",
    if_exists: Literal["overwrite", "append"] = "overwrite",
    partition_cols: list[str] | None = None,
    compression: Literal["snappy", "gzip", "zstd"] | None = None,
    aws_sep: str = ",",
    credentials_secret: str | None = None,
    aws_config_key: str | None = None,
) -> None:
    """Flow for downloading data from Hubspot to Redshift Spectrum.

    Args:
        hubspot_url (str, optional): The base hubspot url of the Hubspot instance.
            Defaults to None.
        to_path (str): Path to a S3 folder where the table will be located.
        schema_name (str): AWS Glue catalog database name.
        table (str): AWS Glue catalog table name.
        api_method (str, optional): The method to use to get the data from the API.
        contact_type (str, optional): The type of contact to get from the data.
            Defaults to "influencedContacts".
        campaign_ids (list[str], optional): List of campaign IDs to get the metrics for.
        filters (list[dict[str, Any]], optional): List of filters to apply to the data.
        properties (list[Any], optional): List of properties to get from the data.
        nrows (int, optional): Number of rows to get from the data.
        hubspot_config_key (Optional[str], optional): The key in the viadot config
             holding relevant Hubspot credentials. Defaults to None.
        hubspot_credentials_secret (str, optional): The name of the secret that stores
            Hubspot credentials. Defaults to None.
        extension (str): Required file type. Accepted file formats are 'csv' and
            'parquet'. Defaults to ".parquet".
        if_exists (Literal["overwrite", "append"]): Whether to overwrite or append to
            existing data. Defaults to "overwrite".
        partition_cols (list[str], optional): List of column names that will be used to
            create partitions. Defaults to None.
        compression (Literal["snappy", "gzip", "zstd"], optional): Compression style
            (None, snappy, gzip, zstd). Defaults to None.
        aws_sep (str, optional): Field delimiter for the output file. Defaults to ','.
        credentials_secret (str, optional): The name of a secret block in Prefect
            that stores AWS credentials. Defaults to None.
        aws_config_key (str, optional): The key in the viadot config holding relevant
            AWS credentials. Defaults to None.

    """
    df = hubspot_to_df(
        endpoint=hubspot_url,
        api_method=api_method,
        contact_type=contact_type,
        campaign_ids=campaign_ids,
        config_key=hubspot_config_key,
        credentials_secret=hubspot_credentials_secret,
        filters=filters,
        properties=properties,
        nrows=nrows,
    )

    return df_to_redshift_spectrum(
        df=df,
        to_path=to_path,
        schema_name=schema_name,
        table=table,
        extension=extension,
        if_exists=if_exists,
        partition_cols=partition_cols,
        compression=compression,
        sep=aws_sep,
        config_key=aws_config_key,
        credentials_secret=credentials_secret,
    )
