"""Flow for pulling data from CloudForCustomers to Adls."""

from typing import Any

from prefect import flow
from prefect.task_runners import ConcurrentTaskRunner

from viadot.orchestration.prefect.tasks import (
    supermetrics_to_df,
    df_to_adls,
)


@flow(
    name="Supermetrics extraction to ADLS",
    description="Extract data from Supermetrics and load it into Azure Data Lake Storage.",
    retries=1,
    retry_delay_seconds=60,
    task_runner=ConcurrentTaskRunner,
)
def supermetrics_to_adls(  # noqa: PLR0913
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
):
    """
    Extracts data from the Supermetrics API and saves it to Azure Data Lake Storage (ADLS).

    This function first queries data from the Supermetrics API based on the provided query parameters
    and then saves the resulting DataFrame to Azure Data Lake Storage (ADLS) as a file. 

    Parameters
    ----------
    query_params : dict[str, Any], optional
        A dictionary containing the query parameters to pass to the Supermetrics API. 
        These parameters define what data to retrieve from Supermetrics. If not provided, 
        the default parameters defined in the Supermetrics configuration will be used.

    adls_path : str, optional
        The destination path in ADLS where the resulting DataFrame will be saved. This should
        include the file name and extension (e.g., 'myfolder/myfile.csv'). If not provided, 
        the function will use a default path defined in the configuration or will raise an error.

    overwrite : bool, default=False
        A flag indicating whether to overwrite the file in ADLS if it already exists. 
        If set to False and the file exists, an error will be raised.

    supermetrics_credentials_secret : str, optional
        The name of the secret in your secret management system that contains the Supermetrics API credentials. 
        If not provided, the function will use credentials specified in the configuration.

    supermetrics_config_key : str, optional
        The configuration key for the Supermetrics API, used to retrieve stored query parameters and credentials. 
        This key corresponds to a section in your configuration files.

    adls_credentials_secret : str, optional
        The name of the secret in your secret management system that contains the ADLS credentials. 
        If not provided, the function will use credentials specified in the configuration.

    adls_config_key : str, optional
        The configuration key for ADLS, used to retrieve stored connection parameters and credentials.
        This key corresponds to a section in your configuration files.

    kwargs : dict[str, Any], optional
        Additional keyword arguments that will be passed to the `supermetrics_to_df` function. 
        This allows further customization of the Supermetrics query.

    Returns
    -------
    None
        This function does not return any value. It performs an ETL operation where data is extracted 
        from Supermetrics, transformed into a DataFrame, and loaded into Azure Data Lake Storage.
    
    Raises
    ------
    ValueError
        If the `adls_path` is not provided and cannot be determined from the configuration.

    Notes
    -----
    This function combines the data extraction and loading process into one step, 
    simplifying the ETL workflow for Supermetrics data into ADLS.
    """
    
    
    df = supermetrics_to_df(
        query_params = query_params,
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
