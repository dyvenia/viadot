"""Download data from Matomo API and load into Redshift Spectrum."""

from typing import Any, Literal

from prefect import flow
from prefect.task_runners import ConcurrentTaskRunner

from viadot.orchestration.prefect.tasks import df_to_redshift_spectrum, matomo_to_df


@flow(
    name="Matomo extraction to Redshift Spectrum",
    description="Extract data from Matomo API and load into Redshift Spectrum.",
    retries=1,
    retry_delay_seconds=60,
    task_runner=ConcurrentTaskRunner,
)
def matomo_to_redshift_spectrum(  # noqa: PLR0913
    matomo_url: str,
    top_level_fields: list[str],
    record_path: str | list[str],
    to_path: str,
    schema_name: str,
    table: str,
    params: dict[str, Any],
    matomo_config_key: str | None = None,
    matomo_credentials_secret: str | None = None,
    record_prefix: str | None = None,
    if_empty: Literal["warn", "skip", "fail"] = "warn",
    tests: dict[str, Any] | None = None,
    extension: str = ".parquet",
    if_exists: Literal["overwrite", "append"] = "overwrite",
    partition_cols: list[str] | None = None,
    compression: Literal["snappy", "gzip", "zstd"] | None = None,
    aws_sep: str = ",",
    credentials_secret: str | None = None,
    aws_config_key: str | None = None,
) -> None:
    """Flow for downloading data from Matomo to Redshift Spectrum.

    Args:
        matomo_url (str): The base matomo url of the Matomo instance.
        top_level_fields (list[str]): List of top level fields to get from the API JSON.
        record_path (str | list[str]): The path field to the records
            in the API response.
            Could be handled as a list of path + fields to extract:
                    record_path = 'actionDetails'
                    record_path = ['actionDetails', 'eventAction']
        to_path (str): Path to a S3 folder where the table will be located.
        schema_name (str): AWS Glue catalog database name.
        table (str): AWS Glue catalog table name.
        matomo_config_key (Optional[str], optional): The key in the viadot config
             holding relevant Matomo credentials. Defaults to None.
        matomo_credentials_secret (str, optional): The name of the secret that stores
            Matomo credentials. Defaults to None.
        params (dict[str, Any]): Parameters for the API request.
                Required params: "module","method","idSite","period","date","format".
        record_prefix (Optional[str], optional): A prefix for the record path fields.
            For example: "action_". Defaults to None.
        if_empty (Literal["warn", "skip", "fail"], optional): What to do if the
            query returns no data. Defaults to "warn".
        tests (dict[str, Any], optional): A dictionary with optional list of tests
            to verify the output dataframe. If defined, triggers the `validate`
            function from viadot.utils. Defaults to None.
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

    Examples:
        matomo_to_redshift_spectrum(
            matomo_url="https://matomo.example.com",
            top_level_fields=["idSite", "visitorId", "visitIp"],
            record_path="actionDetails",
            to_path="s3://your-bucket/matomo/",
            schema_name="staging",
            table="matomo_visits",
            matomo_config_key="matomo_prod",
            params={
                "module": "API",
                "method": "Live.getLastVisitsDetails",
                "idSite": "53",
                "period": "range",
                "date": ("<<yesterday>>", "<<yesterday>>"),
                "format": "JSON"
            },
            tests={
                "unique": ["idsite", "visitIp"],
                "not_null": ["serverDate"],
            },
            partition_cols=["serverDate"],
            compression="snappy",
            if_exists="overwrite",
        )
    """
    data_frame = matomo_to_df(
        url=matomo_url,
        top_level_fields=top_level_fields,
        record_path=record_path,
        config_key=matomo_config_key,
        credentials_secret=matomo_credentials_secret,
        params=params,
        record_prefix=record_prefix,
        if_empty=if_empty,
        tests=tests,
    )

    return df_to_redshift_spectrum(
        df=data_frame,
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
