"""Flow for downloading the data from Superpetrics and uploading it to ADLS."""

from typing import Any

from prefect import flow
from prefect.task_runners import ConcurrentTaskRunner

from viadot.orchestration.prefect.tasks import (
    df_to_adls,
    supermetrics_to_df,
)


@flow(
    name="Supermetrics extraction to ADLS",
    description="Extract data from Supermetrics and load it into ADLS.",
    retries=1,
    retry_delay_seconds=60,
    task_runner=ConcurrentTaskRunner,
)
def supermetrics_to_adls(
    # supermetrics
    query_params: dict[str, Any] | None = None,
    # ADLS
    adls_path: str | None = None,
    overwrite: bool = False,
    # Auth
    supermetrics_credentials_secret: str | None = None,
    supermetrics_config_key: str | None = None,
    adls_credentials_secret: str | None = None,
    adls_config_key: str | None = None,
    **kwargs: dict[str, Any] | None,
) -> None:
    """Flow to extract data from the Supermetrics API and save it to ADLS.

    This function queries data from the Supermetrics API using the provided query
    parameters and saves the resulting DataFrame to Azure Data Lake Storage (ADLS)
    as a file.

    Args:
    ----
        query_params (dict[str, Any], optional):
            A dictionary of query parameters for the Supermetrics API. These parameters
            specify the data to retrieve from Supermetrics. If not provided, the default
            parameters from the Supermetrics configuration will be used.
        adls_path (str, optional):
            The destination path in ADLS where the DataFrame will be saved. This should
            include the file name and extension (e.g., 'myfolder/myfile.csv'). If not
            provided, the function will use a default path from the configuration
            or raise an error.
        overwrite (bool, optional):
            A flag indicating whether to overwrite the existing file in ADLS. If set
            to Falseand the file exists, an error will be raised. Default is False.
        supermetrics_credentials_secret (str, optional):
            The name of the secret in the secret management system containing
            the Supermetrics API credentials. If not provided, the function will use
            credentials specified in the configuration.
        supermetrics_config_key (str, optional):
            The key in the viadot configuration holding relevant credentials.
            Defaults to None.
        adls_credentials_secret (str, optional):
            The name of the secret in the secret management system containing
            the ADLS credentials. If not provided, the function will use credentials
            specified in the configuration.
        adls_config_key (str, optional):
            The key in the viadot configuration holding relevant credentials.
            Defaults to None.
        **kwargs (dict[str, Any], optional):
            Additional keyword arguments to pass to the `supermetrics_to_df` function
            for further customization of the Supermetrics query.

    Raises:
    ------
        ValueError:
            If `adls_path` is not provided and cannot be determined from
            the configuration.

    """
    df = supermetrics_to_df(
        query_params=query_params,
        credentials_secret=supermetrics_credentials_secret,
        config_key=supermetrics_config_key,
        **kwargs,
    )

    return df_to_adls(
        df=df,
        path=adls_path,
        credentials_secret=adls_credentials_secret,
        config_key=adls_config_key,
        overwrite=overwrite,
    )
