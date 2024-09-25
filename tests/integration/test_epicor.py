import pandas as pd
import pytest

from viadot.config import get_source_credentials
from viadot.exceptions import DataRangeError
from viadot.sources import Epicor


FILTERS_NOK = """
    <OrderQuery>
        <QueryFields>
            <CompanyNumber>001</CompanyNumber>
            <BegInvoiceDate></BegInvoiceDate>
            <EndInvoiceDate>2022-05-16</EndInvoiceDate>
            <RecordCount>3</RecordCount>
        </QueryFields>
    </OrderQuery>"""

FILTERS_ORDERS = """
    <OrderQuery>
        <QueryFields>
            <CompanyNumber>001</CompanyNumber>
            <BegInvoiceDate>2022-05-16</BegInvoiceDate>
            <EndInvoiceDate>2022-05-16</EndInvoiceDate>
            <RecordCount>3</RecordCount>
        </QueryFields>
    </OrderQuery>"""
FILTERS_BOOKINGS = """
    <BookingsQuery>
        <QueryFields>
            <CompanyNumber></CompanyNumber>
            <CustomerNumber></CustomerNumber>
            <SellingWarehouse></SellingWarehouse>
            <WrittenBy></WrittenBy>
            <SalesRepOne></SalesRepOne>
            <BegUpdateDate>2022-05-16</BegUpdateDate>
            <EndUpdateDate>2022-05-16</EndUpdateDate>
            <SortXMLTagName></SortXMLTagName>
            <SortMethod></SortMethod>
            <RecordCount></RecordCount>
            <RecordCountPage></RecordCountPage>
        </QueryFields>
    </BookingsQuery>"""
FILTERS_OPEN_ORDERS = """
    <OrderQuery>
        <QueryFields>
            <CompanyNumber></CompanyNumber>
            <CustomerNumber></CustomerNumber>
            <SellingWarehouse></SellingWarehouse>
            <OrderNumber></OrderNumber>
            <WrittenBy></WrittenBy>
            <CustomerPurchaseOrderNumber></CustomerPurchaseOrderNumber>
            <CustomerReleaseNumber></CustomerReleaseNumber>
            <CustomerJobNumber></CustomerJobNumber>
            <EcommerceId></EcommerceId>
            <EcommerceOrderNumber></EcommerceOrderNumber>
            <BegOrderDate></BegOrderDate>
            <EndOrderDate></EndOrderDate>
            <SortXMLTagName></SortXMLTagName>
            <SortMethod></SortMethod>
            <RecordCount></RecordCount>
            <RecordCountPage></RecordCountPage>
        </QueryFields>
    </OrderQuery>"""
FILTERS_CUSTOMERS = """
    <Query>
        <QueryFields>
            <CompanyNumber></CompanyNumber>
            <CustomerNumber></CustomerNumber>
            <SellingWarehouse></SellingWarehouse>
            <City></City>
            <State></State>
            <ZipCode></ZipCode>
            <EmailAddress></EmailAddress>
            <Phone></Phone>
            <BegLastChangeDate></BegLastChangeDate>
            <EndLastChangeDate></EndLastChangeDate>
            <SortXMLTagName>1</SortXMLTagName>
            <SortMethod></SortMethod>
            <RecordCount></RecordCount>
            <RecordCountPage></RecordCountPage>
        </QueryFields>
    </Query>"""


@pytest.fixture(scope="session")
def epicor():
    return Epicor(
        base_url=get_source_credentials("epicor").get("test_url"),
        config_key="epicor",
        validate_date_filter=False,
    )


def test_connection(epicor: Epicor):
    assert epicor.get_xml_response(FILTERS_ORDERS).ok


def test_validate_filter(epicor: Epicor):
    with pytest.raises(DataRangeError):
        epicor.validate_filter(FILTERS_NOK)


def test_to_df_orders(epicor: Epicor):
    df = epicor.to_df(FILTERS_ORDERS)
    assert isinstance(df, pd.DataFrame)


def test_to_df_bookings(epicor: Epicor):
    df = epicor.to_df(FILTERS_BOOKINGS)
    assert isinstance(df, pd.DataFrame)


def test_to_df_open_orders(epicor: Epicor):
    df = epicor.to_df(FILTERS_OPEN_ORDERS)
    assert isinstance(df, pd.DataFrame)


def test_to_df_customers(epicor: Epicor):
    df = epicor.to_df(FILTERS_CUSTOMERS)
    assert isinstance(df, pd.DataFrame)
