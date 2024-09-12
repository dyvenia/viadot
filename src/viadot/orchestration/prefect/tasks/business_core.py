from typing import Any, Dict
from venv import logger
from viadot.config import get_source_credentials
from viadot.orchestration.prefect.exceptions import MissingSourceCredentialsError
from viadot.orchestration.prefect.utils import get_credentials
from viadot.sources.business_core import BusinessCore
from prefect import task


@task(retries=3, retry_delay_seconds=10, timeout_seconds=60 * 60 * 3)
def business_core_to_df(
    path: str,
    url: str,
    filters_dict: Dict[str, Any] = {
        "BucketCount": None,
        "BucketNo": None,
        "FromDate": None,
        "ToDate": None,
    },
    credentials_secret: str = None,
    config_key: str = "BusinessCore",
    if_empty: str = "skip",
): 
    """Task for downloading  data from Business Core API to a Parquet file.

    Args:
        path (str, required): Path where to save the Parquet file. Defaults to None.
        url (str, required): Base url to the view in Business Core API. Defaults to None.
        filters_dict (Dict[str, Any], optional): Filters in form of dictionary. Available filters: 'BucketCount',
            'BucketNo', 'FromDate', 'ToDate'.  Defaults to {"BucketCount": None,"BucketNo": None,"FromDate": None,
            "ToDate": None,}. Defaults to None.
        credentials_secret (str, optional): The name of the secret that stores SAP credentials. Defaults to None.
            More info on: https://docs.prefect.io/concepts/blocks/
        config_key (str, optional): Credential key to dictionary where details are stored. Defaults to "BusinessCore".
        if_empty (str, optional): What to do if output DataFrame is empty. Defaults to "skip".
    """

    if not (credentials_secret or config_key):
        raise MissingSourceCredentialsError
    
    credentials = get_source_credentials(config_key) or get_credentials(credentials_secret)


    bc = BusinessCore(
            url=url,
            credentials=credentials,
            config_key=config_key,
            filters_dict=filters_dict,
        )

    df = bc.to_df(if_empty = if_empty)
        
    nrows = df.shape[0]
    ncols = df.shape[1]

    logger.info(
        f"Successfully downloaded {nrows} rows and {ncols} columns of data to a DataFrame."
    )
    
    return df

