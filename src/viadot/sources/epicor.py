"""Source for connecting to Epicor Prelude API."""

from typing import Any

import defusedxml.ElementTree as ET  # noqa: N817
import pandas as pd
from pydantic import BaseModel
import requests

from viadot.config import get_source_credentials
from viadot.exceptions import DataRangeError, ValidationError
from viadot.sources.base import Source
from viadot.utils import handle_api_response


"""The official documentation does not specify the list of required
fields so they were set as optional in BaseModel classes.

Each Epicor Prelude view requires different XML parser.
"""


class TrackingNumbers(BaseModel):
    TrackingNumber: str | None


class ShipToAddress(BaseModel):
    ShipToNumber: str | None
    Attention: str | None
    AddressLine1: str | None
    AddressLine2: str | None
    AddressLine3: str | None
    City: str | None
    State: str | None
    Zip: str | None
    Country: str | None
    EmailAddress: str | None
    PhoneNumber: str | None
    FaxNumber: str | None


class InvoiceTotals(BaseModel):
    Merchandise: str | None
    InboundFreight: str | None
    OutboundFreight: str | None
    Handling: str | None
    Delivery: str | None
    Pickup: str | None
    Restocking: str | None
    MinimumCharge: str | None
    DiscountAllowance: str | None
    SalesTax: str | None
    TotalInvoice: str | None


class HeaderInformation(BaseModel):
    CompanyNumber: str | None
    OrderNumber: str | None
    InvoiceNumber: str | None
    CustomerNumber: str | None
    CustomerDescription: str | None
    CustomerPurchaseOrderNumber: str | None
    Contact: str | None
    SellingWarehouse: str | None
    ShippingWarehouse: str | None
    ShippingMethod: str | None
    PaymentTerms: str | None
    PaymentTermsDescription: str | None
    FreightTerms: str | None
    FreightTermsDescription: str | None
    SalesRepOne: str | None
    SalesRepOneDescription: str | None
    EntryDate: str | None
    OrderDate: str | None
    RequiredDate: str | None
    ShippedDate: str | None
    InvoiceDate: str | None
    ShipToAddress: ShipToAddress | None
    TrackingNumbers: TrackingNumbers | None
    InvoiceTotals: InvoiceTotals | None


class LineItemDetail(BaseModel):
    ProductNumber: str | None
    ProductDescription: str | None
    ProductDescription1: str | None
    ProductDescription2: str | None
    CustomerProductNumber: str | None
    LineItemNumber: str | None
    QuantityOrdered: str | None
    QuantityShipped: str | None
    QuantityBackordered: str | None
    Price: str | None
    ExtendedCost: str | None
    ExtendedPrice: str | None
    QuantityShippedExtension: str | None
    LineItemShipWarehouse: str | None
    RequiredDate: str | None
    CopperWeight: str | None
    UnitOfMeasure: str | None
    Status: str | None
    GrossProfitExtension: str | None
    GrossProfitPercent: str | None
    UpdateDate: str | None
    DeleteReasonCode: str | None
    DeleteReasonDescription: str | None
    VendorProductNumber: str | None
    ProductGroup: str | None
    ProductGroupDescription: str | None
    ProductLine: str | None
    ProductLineDescription: str | None


class Customer(BaseModel):
    CompanyNumber: str | None
    CustomerNumber: str | None
    Description: str | None
    AddressOne: str | None
    AddressTwo: str | None
    AddressThree: str | None
    City: str | None
    State: str | None
    Zip: str | None
    Country: str | None
    Contact: str | None
    Phone: str | None
    EmailAddress: str | None
    ShipTos: ShipToAddress | None


class Order(BaseModel):
    HeaderInformation: HeaderInformation | None
    LineItemDetail: LineItemDetail | None


class BookingsInfo(BaseModel):
    HeaderInformation: HeaderInformation | None
    LineItemDetail: LineItemDetail | None


def parse_orders_xml(xml_data: str) -> pd.DataFrame:  # noqa: C901, PLR0912
    """Function to parse xml containing Epicor Orders Data.

    Args:
        xml_data (str, required): Response from Epicor API in form of xml

    Returns:
        pd.DataFrame: DataFrame containing parsed orders data.
    """
    dfs = []
    ship_dict = {}
    invoice_dict = {}
    header_params_dict = {}
    item_params_dict = {}

    root = ET.fromstring(xml_data.text)

    for order in root.findall("Order"):
        for header in order.findall("HeaderInformation"):
            for tracking_numbers in header.findall("TrackingNumbers"):
                numbers = ""
                for tracking_number in tracking_numbers.findall("TrackingNumber"):
                    numbers = numbers + "'" + tracking_number.text + "'"
                result_numbers = TrackingNumbers(TrackingNumber=numbers)

            for shipto in header.findall("ShipToAddress"):
                for ship_param in ShipToAddress.__dict__.get("__annotations__"):
                    try:
                        ship_value = shipto.find(ship_param).text
                    except AttributeError:
                        ship_value = None
                    ship_parameter = {ship_param: ship_value}
                    ship_dict.update(ship_parameter)
                ship_address = ShipToAddress(**ship_dict)

            for invoice in header.findall("InvoiceTotals"):
                for invoice_param in InvoiceTotals.__dict__.get("__annotations__"):
                    try:
                        invoice_value = invoice.find(invoice_param).text
                    except AttributeError:
                        invoice_value = None
                    invoice_parameter = {invoice_param: invoice_value}
                    invoice_dict.update(invoice_parameter)
                invoice_total = InvoiceTotals(**invoice_dict)

            for header_param in HeaderInformation.__dict__.get("__annotations__"):
                try:
                    header_value = header.find(header_param).text
                except AttributeError:
                    header_value = None
                if header_param == "TrackingNumbers":
                    header_parameter = {header_param: result_numbers}
                elif header_param == "ShipToAddress":
                    header_parameter = {header_param: ship_address}
                elif header_param == "InvoiceTotals":
                    header_parameter = {header_param: invoice_total}
                else:
                    header_parameter = {header_param: header_value}
                header_params_dict.update(header_parameter)
            header_info = HeaderInformation(**header_params_dict)
        for items in order.findall("LineItemDetails"):
            for item in items.findall("LineItemDetail"):
                for item_param in LineItemDetail.__dict__.get("__annotations__"):
                    try:
                        item_value = item.find(item_param).text
                    except AttributeError:
                        item_value = None
                    item_parameter = {item_param: item_value}
                    item_params_dict.update(item_parameter)
                line_item = LineItemDetail(**item_params_dict)
                row = Order(HeaderInformation=header_info, LineItemDetail=line_item)
                my_dict = row.dict()
                dfs.append(pd.json_normalize(my_dict, max_level=2))
    return pd.concat(dfs, ignore_index=True)


def parse_bookings_xml(xml_data: str) -> pd.DataFrame:  # noqa: C901, PLR0912
    """Function to parse xml containing Epicor Bookings Data.

    Args:
        xml_data (str, required): Response from Epicor API in form of xml
    Returns:
        pd.DataFrame: DataFrame containing parsed  data.
    """
    dfs = []
    ship_dict = {}
    header_params_dict = {}
    item_params_dict = {}
    root = ET.fromstring(xml_data.text)

    for booking in root.findall("BookingsInfo"):
        for header in booking.findall("HeaderInformation"):
            for shipto in header.findall("ShipToAddress"):
                for ship_param in ShipToAddress.__dict__.get("__annotations__"):
                    try:
                        ship_value = shipto.find(ship_param).text
                    except AttributeError:
                        ship_value = None
                    ship_parameter = {ship_param: ship_value}
                    ship_dict.update(ship_parameter)
                ship_address = ShipToAddress(**ship_dict)

            for header_param in HeaderInformation.__dict__.get("__annotations__"):
                try:
                    header_value = header.find(header_param).text
                except AttributeError:
                    header_value = None
                if header_param == "ShipToAddress":
                    header_parameter = {header_param: ship_address}
                else:
                    header_parameter = {header_param: header_value}
                header_params_dict.update(header_parameter)
            header_info = HeaderInformation(**header_params_dict)
        for items in booking.findall("LineItemDetails"):
            for item in items.findall("LineItemDetail"):
                for item_param in LineItemDetail.__dict__.get("__annotations__"):
                    try:
                        item_value = item.find(item_param).text
                    except AttributeError:
                        item_value = None
                    item_parameter = {item_param: item_value}
                    item_params_dict.update(item_parameter)
                line_item = LineItemDetail(**item_params_dict)
                row = BookingsInfo(
                    HeaderInformation=header_info, LineItemDetail=line_item
                )
                my_dict = row.dict()
                dfs.append(pd.json_normalize(my_dict, max_level=2))
    return pd.concat(dfs, ignore_index=True)


def parse_open_orders_xml(xml_data: str) -> pd.DataFrame:  # noqa: C901, PLR0912
    """Function to parse xml containing Epicor Open Orders Data.

    Args:
        xml_data (str, required): Response from Epicor API in form of xml
    Returns:
        pd.DataFrame: DataFrame containing parsed  data.
    """
    dfs = []
    ship_dict = {}
    header_params_dict = {}
    item_params_dict = {}

    root = ET.fromstring(xml_data.text)

    for order in root.findall("Order"):
        for header in order.findall("HeaderInformation"):
            for shipto in header.findall("ShipToAddress"):
                for ship_param in ShipToAddress.__dict__.get("__annotations__"):
                    try:
                        ship_value = shipto.find(ship_param).text
                    except AttributeError:
                        ship_value = None
                    ship_parameter = {ship_param: ship_value}
                    ship_dict.update(ship_parameter)
                ship_address = ShipToAddress(**ship_dict)

            for header_param in HeaderInformation.__dict__.get("__annotations__"):
                try:
                    header_value = header.find(header_param).text
                except AttributeError:
                    header_value = None
                if header_param == "ShipToAddress":
                    header_parameter = {header_param: ship_address}
                else:
                    header_parameter = {header_param: header_value}
                header_params_dict.update(header_parameter)
            header_info = HeaderInformation(**header_params_dict)
        for items in order.findall("LineItemDetails"):
            for item in items.findall("LineItemDetail"):
                for item_param in LineItemDetail.__dict__.get("__annotations__"):
                    try:
                        item_value = item.find(item_param).text
                    except AttributeError:
                        item_value = None
                    item_parameter = {item_param: item_value}
                    item_params_dict.update(item_parameter)
                line_item = LineItemDetail(**item_params_dict)
                row = Order(HeaderInformation=header_info, LineItemDetail=line_item)
                my_dict = row.dict()
                dfs.append(pd.json_normalize(my_dict, max_level=2))
    return pd.concat(dfs, ignore_index=True)


def parse_customer_xml(xml_data: str) -> pd.DataFrame:
    """Function to parse xml containing Epicor Customers Data.

    Args:
        xml_data (str, required): Response from Epicor API in form of xml
    Returns:
        pd.DataFrame: DataFrame containing parsed  data.
    """
    dfs = []
    ship_dict = {}
    customer_params_dict = {}

    root = ET.fromstring(xml_data.text)

    for customer in root.findall("Customer"):
        for ship in customer.findall("ShipTos"):
            for shipto in ship.findall("ShipToAddress"):
                for ship_param in ShipToAddress.__dict__.get("__annotations__"):
                    try:
                        ship_value = shipto.find(ship_param).text
                    except AttributeError:
                        ship_value = None
                    ship_parameter = {ship_param: ship_value}
                    ship_dict.update(ship_parameter)
                ship_address = ShipToAddress(**ship_dict)

                for cust_param in Customer.__dict__.get("__annotations__"):
                    try:
                        cust_value = customer.find(cust_param).text
                    except AttributeError:
                        cust_value = None
                    if cust_param == "ShipTos":
                        cust_parameter = {cust_param: ship_address}
                    else:
                        cust_parameter = {cust_param: cust_value}
                    customer_params_dict.update(cust_parameter)
                cust_info = Customer(**customer_params_dict)
                my_dict = cust_info.dict()
                dfs.append(pd.json_normalize(my_dict, max_level=2))
    return pd.concat(dfs, ignore_index=True)


class EpicorCredentials(BaseModel):
    host: str
    port: int = 443
    username: str
    password: str


class Epicor(Source):
    def __init__(
        self,
        base_url: str,
        credentials: dict[str, Any] | None = None,
        config_key: str | None = None,
        validate_date_filter: bool = True,
        start_date_field: str = "BegInvoiceDate",
        end_date_field: str = "EndInvoiceDate",
        *args,
        **kwargs,
    ):
        """Class to connect to Epicor API and pasere XML output into a pandas DataFrame.

        Args:
            base_url (str, required): Base url to Epicor.
            filters_xml (str, required): Filters in form of XML. The date filter
                 is required.
            credentials (dict[str, Any], optional): Credentials to connect with
            Epicor API containing host, port, username and password.Defaults to None.
            config_key (str, optional): Credential key to dictionary where details
                 are stored.
            validate_date_filter (bool, optional): Whether or not validate xml
                date filters. Defaults to True.
            start_date_field (str, optional) The name of filters field containing
                start date.Defaults to "BegInvoiceDate".
            end_date_field (str, optional) The name of filters field containing
                end date. Defaults to "EndInvoiceDate".
        """
        raw_creds = credentials or get_source_credentials(config_key)
        validated_creds = dict(EpicorCredentials(**raw_creds))

        self.base_url = base_url
        self.validate_date_filter = validate_date_filter
        self.start_date_field = start_date_field
        self.end_date_field = end_date_field

        self.url = (
            "http://"
            + validated_creds["host"]
            + ":"
            + str(validated_creds["port"])
            + base_url
        )

        super().__init__(*args, credentials=validated_creds, **kwargs)

    def generate_token(self) -> str:
        """Function to generate API access token that is valid for 24 hours.

        Returns:
            str: Generated token.
        """
        url = (
            "http://"
            + self.credentials["host"]
            + ":"
            + str(self.credentials["port"])
            + "/api/security/token/"
        )

        headers = {
            "Content-Type": "application/json",
            "username": self.credentials["username"],
            "password": self.credentials["password"],
        }

        response = handle_api_response(url=url, headers=headers, method="POST")
        root = ET.fromstring(response.text)
        return root.find("AccessToken").text

    def validate_filter(self, filters_xml: str) -> None:
        "Function checking if user had specified date range filters."
        root = ET.fromstring(filters_xml)
        for child in root:
            for subchild in child:
                if (
                    subchild.tag in (self.start_date_field, self.end_date_field)
                ) and subchild.text is None:
                    msg = "Too much data. Please provide a date range filter."
                    raise DataRangeError(msg)

    def get_xml_response(self, filters_xml: str) -> requests.models.Response:
        "Function for getting response from Epicor API."
        if self.validate_date_filter is True:
            self.validate_filter(filters_xml)
        payload = filters_xml
        headers = {
            "Content-Type": "application/xml",
            "Authorization": "Bearer " + self.generate_token(),
        }
        return handle_api_response(
            url=self.url, headers=headers, data=payload, method="POST"
        )

    def to_df(self, filters_xml: str) -> pd.DataFrame:
        """Function for creating pandas DataFrame from Epicor API response.

        Returns:
            pd.DataFrame: Output DataFrame.
        """
        data = self.get_xml_response(filters_xml)
        if "ORDER.HISTORY.DETAIL.QUERY" in self.base_url:
            df = parse_orders_xml(data)
        elif "CUSTOMER.QUERY" in self.base_url:
            df = parse_customer_xml(data)
        elif "ORDER.DETAIL.PROD.QUERY" in self.base_url:
            df = parse_open_orders_xml(data)
        elif "BOOKINGS.DETAIL.QUERY" in self.base_url:
            df = parse_bookings_xml(data)
        else:
            msg = f"Parser for selected viev {self.base_url} is not avaiable"
            raise ValidationError(msg)

        return df
