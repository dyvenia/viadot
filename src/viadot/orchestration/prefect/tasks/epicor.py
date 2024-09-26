"""Task for downloading data from Epicor Prelude API."""

import pandas as pd
from prefect import task
from prefect.logging import get_run_logger

from viadot.config import get_source_credentials
from viadot.orchestration.prefect.exceptions import MissingSourceCredentialsError
from viadot.orchestration.prefect.utils import get_credentials
from viadot.sources.epicor import Epicor


@task(retries=3, retry_delay_seconds=10, timeout_seconds=60 * 60 * 3)
def epicor_to_df(
    base_url: str,
    filters_xml: str,
    validate_date_filter: bool = True,
    start_date_field: str = "BegInvoiceDate",
    end_date_field: str = "EndInvoiceDate",
    credentials_secret: str | None = None,
    config_key: str | None = None,
) -> pd.DataFrame:
    """Load the data from Epicor Prelude API into a pandas DataFrame.

    Args:
        base_url (str, required): Base url to Epicor.
        filters_xml (str, required): Filters in form of XML. The date filter
             is required.
        validate_date_filter (bool, optional): Whether or not validate xml date filters.
                Defaults to True.
        start_date_field (str, optional) The name of filters field containing
            start date. Defaults to "BegInvoiceDate".
        end_date_field (str, optional) The name of filters field containing end date.
                Defaults to "EndInvoiceDate".
        credentials_secret (str, optional): The name of the secret storing
            the credentials. Defaults to None.
            More info on: https://docs.prefect.io/concepts/blocks/
        config_key (str, optional): The key in the viadot config holding relevant
            credentials. Defaults to None.

    """
    if not (credentials_secret or config_key):
        raise MissingSourceCredentialsError

    logger = get_run_logger()

    credentials = get_source_credentials(config_key) or get_credentials(
        credentials_secret
    )
    epicor = Epicor(
        credentials=credentials,
        base_url=base_url,
        validate_date_filter=validate_date_filter,
        start_date_field=start_date_field,
        end_date_field=end_date_field,
    )
    df = epicor.to_df(filters_xml=filters_xml)
    nrows = df.shape[0]
    ncols = df.shape[1]

    logger.info(
        f"Successfully downloaded {nrows} rows and {ncols} columns of data to a DataFrame."
    )
    return df
