"""Flows for downloading data from Epicor Prelude API to Parquet file."""

from typing import Literal

from prefect import flow

from viadot.orchestration.prefect.tasks import epicor_to_df
from viadot.orchestration.prefect.tasks.task_utils import df_to_parquet


@flow(
    name="extract--epicor--parquet",
    description="Extract data from Epicor Prelude API and load it into Parquet file",
    retries=1,
    retry_delay_seconds=60,
)
def epicor_to_parquet(
    path: str,
    base_url: str,
    filters_xml: str,
    if_exists: Literal["append", "replace", "skip"] = "replace",
    validate_date_filter: bool = True,
    start_date_field: str = "BegInvoiceDate",
    end_date_field: str = "EndInvoiceDate",
    epicor_credentials_secret: str | None = None,
    epicor_config_key: str | None = None,
) -> None:
    """Download a pandas `DataFrame` from Epicor Prelude API load it into Parquet file.

    Args:
        path (str): Path to Parquet file, where the data will be located.
            Defaults to None.
        base_url (str, required): Base url to Epicor.
        filters_xml (str, required): Filters in form of XML. The date filter
            is required.
        if_exists (Literal["append", "replace", "skip"], optional): Information what
            has to be done, if the file exists. Defaults to "replace"
        validate_date_filter (bool, optional): Whether or not validate xml date filters.
                Defaults to True.
        start_date_field (str, optional) The name of filters field containing
            start date. Defaults to "BegInvoiceDate".
        end_date_field (str, optional) The name of filters field containing end date.
                Defaults to "EndInvoiceDate".
        epicor_credentials_secret (str, optional): The name of the secret storing
            the credentials. Defaults to None.
            More info on: https://docs.prefect.io/concepts/blocks/
        epicor_config_key (str, optional): The key in the viadot config holding relevant
            credentials. Defaults to None.

    Examples:
        >>> epicor_to_parquet(
        >>>     path = "my_parquet.parquet",
        >>>     base_url = "/api/data/import/ORDER.QUERY",
        >>>     filters_xml = "<OrderQuery>
        >>>            <QueryFields>
        >>>                <CompanyNumber>001</CompanyNumber>
        >>>                <CustomerNumber></CustomerNumber>
        >>>                <SellingWarehouse></SellingWarehouse>
        >>>                <OrderNumber></OrderNumber>
        >>>                <WrittenBy></WrittenBy>
        >>>                <CustomerPurchaseOrderNumber></CustomerPurchaseOrderNumber>
        >>>                <CustomerReleaseNumber></CustomerReleaseNumber>
        >>>                <CustomerJobNumber></CustomerJobNumber>
        >>>                <InvoiceNumber></InvoiceNumber>
        >>>                <EcommerceId></EcommerceId>
        >>>                <EcommerceOrderNumber></EcommerceOrderNumber>
        >>>                <QuoteNumber></QuoteNumber>
        >>>                <BegInvoiceDate>{yesterday}</BegInvoiceDate>
        >>>                <EndInvoiceDate>{yesterday}</EndInvoiceDate>
        >>>                <SortXMLTagName></SortXMLTagName>
        >>>                <SortMethod></SortMethod>
        >>>                <RecordCount></RecordCount>
        >>>                <RecordCountPage></RecordCountPage>
        >>>            </QueryFields>
        >>>        </OrderQuery>",
        >>>     epicor_config_key = "epicor"
        >>> )
    """
    df = epicor_to_df(
        base_url=base_url,
        filters_xml=filters_xml,
        validate_date_filter=validate_date_filter,
        start_date_field=start_date_field,
        end_date_field=end_date_field,
        credentials_secret=epicor_credentials_secret,
        config_key=epicor_config_key,
    )

    return df_to_parquet(
        df=df,
        path=path,
        if_exists=if_exists,
    )
