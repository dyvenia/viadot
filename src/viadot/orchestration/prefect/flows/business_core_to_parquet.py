from typing import Any, Dict, Literal
from viadot.config import get_source_credentials
from viadot.orchestration.prefect.exceptions import MissingSourceCredentialsError
from viadot.orchestration.prefect.tasks.business_core import business_core_to_df
from viadot.orchestration.prefect.tasks.task_utils import df_to_parquet
from viadot.orchestration.prefect.utils import get_credentials
from viadot.sources.business_core import BusinessCore
from prefect import flow


@flow(
    name="extract--businesscore--parquet",
    description="Extract data from Business Core API and load it into Parquet file",
    retries=1,
    retry_delay_seconds=60,
)
def business_core_to_parquet(
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
    if_exists: Literal["append", "replace", "skip"] = "replace",
    verify: bool = True,
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
        if_exists (Literal["append", "replace", "skip"], optional): What to do if the table exists. 
            Defaults to "replace".
        verify (bool, optional): Whether or not verify certificates while connecting to an API. Defaults to True.
    """

    if not (credentials_secret or config_key):
        raise MissingSourceCredentialsError
    
    df = business_core_to_df(
            url=url,
            path=path,
            credentials_secret=credentials_secret,
            config_key=config_key,
            filters_dict=filters_dict,
            verify=verify
        )
    return df_to_parquet(
        df=df,
        path=path, 
        if_exists=if_exists,
        )

