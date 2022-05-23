import pandas as pd
import xml.etree.ElementTree as ET
from xml.etree.ElementTree import fromstring

from prefect import Task
from prefect.utilities.tasks import defaults_from_attrs
from typing import Any, Dict, List, Optional

from ..sources import Epicor

from pydantic import BaseModel


class HeaderInformation(BaseModel):
    CompanyNumber: Optional[str]
    OrderNumber: Optional[str]
    InvoiceNumber: Optional[str]
    CustomerNumber: Optional[str]
    CustomerDescription: Optional[str]
    CustomerPurchaseOrderNumber: Optional[str]
    Contact: Optional[str]
    SellingWarehouse: Optional[str]
    ShippingWarehouse: Optional[str]
    ShippingMethod: Optional[str]
    PaymentTerms: Optional[str]
    PaymentTermsDescription: Optional[str]
    FreightTermsDescription: Optional[str]
    SalesRepOne: Optional[str]
    SalesRepOneDescription: Optional[str]
    EntryDate: Optional[str]
    OrderDate: Optional[str]
    RequiredDate: Optional[str]
    ShippedDate: Optional[str]
    InvoiceDate: Optional[str]
    ShipToAddress: Optional[List[Any]]
    TrackingNumbers: Optional[List[Any]]
    InvoiceTotals: Optional[List[Any]]


class ShipToAddress(BaseModel):
    ShipToNumber: Optional[str]
    Attention: Optional[str]
    AddressLine1: Optional[str]
    AddressLine2: Optional[str]
    City: Optional[str]
    State: Optional[str]
    Zip: Optional[str]
    Country: Optional[str]
    EmailAddress: Optional[str]
    PhoneNumber: Optional[str]
    FaxNumber: Optional[str]


class TrackingNumbers(BaseModel):
    TrackingNumber: Optional[str]


class InvoiceTotals(BaseModel):
    Merchandise: Optional[str]
    InboundFreight: Optional[str]
    OutboundFreight: Optional[str]
    Handling: Optional[str]
    Delivery: Optional[str]
    Pickup: Optional[str]
    Restocking: Optional[str]
    MinimumCharge: Optional[str]
    DiscountAllowance: Optional[str]
    SalesTax: Optional[str]
    TotalInvoice: Optional[str]


class LineItemDetail(BaseModel):
    ProductNumber: Optional[str]
    ProductDescription1: Optional[str]
    ProductDescription2: Optional[str]
    LineItemNumber: Optional[str]
    QuantityOrdered: Optional[str]
    QuantityShipped: Optional[str]
    Price: Optional[str]
    UnitOfMeasure: Optional[str]
    ExtendedPrice: Optional[str]
    QuantityShippedExtension: Optional[str]
    LineItemShipWarehouse: Optional[str]


class LineItemDetails(BaseModel):
    LineItemDetail: Optional[LineItemDetail]


class Order(BaseModel):
    HeaderInformation: Optional[HeaderInformation]
    LineItemDetails: Optional[LineItemDetails]


class Orders(BaseModel):
    Order: Optional[List[Any]]


def parse_orders_xml(xml_data: str) -> pd.DataFrame:
    """
    Function to parse xml containing Epicor Orders Data.

    Args:
        xml_data (str, required): Response from Epicor API in form of xml
    Returns:
        pd.DataFrame: DataFrame containing parsed orders data.

    """
    full_df = pd.DataFrame()
    ship_dict = {}
    invoice_dict = {}
    header_params_dict = {}
    item_params_dict = {}
    almost_full = {}

    root = ET.fromstring(xml_data.text)
    for order in root.findall("Order"):
        for header in order.findall("HeaderInformation"):
            for tracking_numbers in header.findall("TrackingNumbers"):
                numbers = ""
                for tracking_number in tracking_numbers.findall("TrackingNumber"):
                    numbers = numbers + "'" + tracking_number.text + "'"
                result_numbers = TrackingNumbers(TrackingNumber=numbers)
                almost_full.update(result_numbers)

            for shipto in header.findall("ShipToAddress"):
                for ship_param in ShipToAddress.__dict__.get("__annotations__"):
                    try:
                        ship_value = shipto.find(f"{ship_param}").text
                    except:
                        ship_value = None
                    ship_parameter = {ship_param: ship_value}
                    ship_dict.update(ship_parameter)
                almost_full.update(ship_dict)

            for invoice in header.findall("InvoiceTotals"):
                for invoice_param in InvoiceTotals.__dict__.get("__annotations__"):
                    try:
                        invoice_value = invoice.find(f"{invoice_param}").text
                    except:
                        invoice_value = None
                    invoice_parameter = {invoice_param: invoice_value}
                    invoice_dict.update(invoice_parameter)
                almost_full.update(invoice_dict)

            for header_param in HeaderInformation.__dict__.get("__annotations__"):
                try:
                    header_value = header.find(f"{header_param}").text
                except:
                    header_value = None
                header_parameter = {header_param: header_value}
                header_params_dict.update(header_parameter)
            almost_full.update(header_params_dict)

        for items in order.findall("LineItemDetails"):
            for item in items.findall("LineItemDetail"):
                for item_param in LineItemDetail.__dict__.get("__annotations__"):
                    try:
                        item_value = item.find(f"{item_param}").text
                    except:
                        item_value = None
                    item_parameter = {item_param: item_value}
                    item_params_dict.update(item_parameter)
                almost_full.update(item_params_dict)
                full_df = full_df.append(almost_full, ignore_index=True)
    return full_df


class EpicorOrdersToDF(Task):
    def __init__(
        self,
        base_url: str,
        filters_xml: str,
        credentials: Dict[str, Any] = None,
        config_key: str = None,
        start_date_field: str = "BegInvoiceDate",
        end_date_field: str = "EndInvoiceDate",
        *args,
        **kwargs,
    ) -> pd.DataFrame:
        """
        Task for downloading and parsing orders data from Epicor API to a pandas DataFrame.

        Args:
            name (str): The name of the flow.
            base_url (str, required): Base url to Epicor Orders.
            filters_xml (str, required): Filters in form of XML. The date filter is necessary.
            credentials (Dict[str, Any], optional): Credentials to connect with Epicor Api containing host, port, username and password. Defaults to None.
            config_key (str, optional): Credential key to dictionary where details are stored. Defauls to None.
            start_date_field (str, optional) The name of filters filed containing start date. Defaults to "BegInvoiceDate".
            end_date_field (str, optional) The name of filters filed containing end date. Defaults to "EndInvoiceDate".

        Returns:
            pd.DataFrame: DataFrame with parsed API output
        """
        self.credentials = credentials
        self.config_key = config_key
        self.base_url = base_url
        self.filters_xml = filters_xml
        self.start_date_field = start_date_field
        self.end_date_field = end_date_field
        super().__init__(
            name="EpicorOrders_to_df",
            *args,
            **kwargs,
        )

    def __call__(self, *args, **kwargs):
        """Load Epicor Orders to DF"""
        return super().__call__(*args, **kwargs)

    @defaults_from_attrs(
        "credentials",
        "config_key",
        "base_url",
        "filters_xml",
        "start_date_field",
        "end_date_field",
    )
    def run(
        self,
        credentials: Dict[str, Any] = None,
        config_key: str = None,
        base_url: str = None,
        filters_xml: str = None,
        start_date_field: str = None,
        end_date_field: str = None,
    ):
        epicor = Epicor(
            credentials=credentials,
            config_key=config_key,
            base_url=base_url,
            filters_xml=filters_xml,
            start_date_field=start_date_field,
            end_date_field=end_date_field,
        )
        data = epicor.get_xml_response()
        df = parse_orders_xml(data)
        return df
