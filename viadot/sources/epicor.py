import xml.etree.ElementTree as ET
from typing import Any, Dict, Optional

import pandas as pd
from pydantic import BaseModel

from ..config import local_config
from ..exceptions import CredentialError, DataRangeError
from ..utils import handle_api_response
from .base import Source

""" 
The official documentation does not specify the list of required 
fields so they were set as optional in BaseModel classes.
"""


class TrackingNumbers(BaseModel):
    TrackingNumber: Optional[str]


class ShipToAddress(BaseModel):
    ShipToNumber: Optional[str]
    Attention: Optional[str]
    AddressLine1: Optional[str]
    AddressLine2: Optional[str]
    AddressLine3: Optional[str]
    City: Optional[str]
    State: Optional[str]
    Zip: Optional[str]
    Country: Optional[str]
    EmailAddress: Optional[str]
    PhoneNumber: Optional[str]
    FaxNumber: Optional[str]


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
    FreightTerms: Optional[str]
    FreightTermsDescription: Optional[str]
    SalesRepOne: Optional[str]
    SalesRepOneDescription: Optional[str]
    EntryDate: Optional[str]
    OrderDate: Optional[str]
    RequiredDate: Optional[str]
    ShippedDate: Optional[str]
    InvoiceDate: Optional[str]
    ShipToAddress: Optional[ShipToAddress]
    TrackingNumbers: Optional[TrackingNumbers]
    InvoiceTotals: Optional[InvoiceTotals]


class LineItemDetail(BaseModel):
    ProductNumber: Optional[str]
    ProductDescription1: Optional[str]
    ProductDescription2: Optional[str]
    CustomerProductNumber: Optional[str]
    LineItemNumber: Optional[str]
    QuantityOrdered: Optional[str]
    QuantityShipped: Optional[str]
    QuantityBackordered: Optional[str]
    Price: Optional[str]
    UnitOfMeasure: Optional[str]
    ExtendedPrice: Optional[str]
    QuantityShippedExtension: Optional[str]
    LineItemShipWarehouse: Optional[str]
    RequiredDate: Optional[str]
    CopperWeight: Optional[str]


class Order(BaseModel):
    HeaderInformation: Optional[HeaderInformation]
    LineItemDetail: Optional[LineItemDetail]


def parse_orders_xml(xml_data: str) -> pd.DataFrame:
    """
    Function to parse xml containing Epicor Orders Data.

    Args:
        xml_data (str, required): Response from Epicor API in form of xml
    Returns:
        pd.DataFrame: DataFrame containing parsed orders data.
    """
    final_df = pd.DataFrame()
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
                    except:
                        ship_value = None
                    ship_parameter = {ship_param: ship_value}
                    ship_dict.update(ship_parameter)
                ship_address = ShipToAddress(**ship_dict)

            for invoice in header.findall("InvoiceTotals"):
                for invoice_param in InvoiceTotals.__dict__.get("__annotations__"):
                    try:
                        invoice_value = invoice.find(invoice_param).text
                    except:
                        invoice_value = None
                    invoice_parameter = {invoice_param: invoice_value}
                    invoice_dict.update(invoice_parameter)
                invoice_total = InvoiceTotals(**invoice_dict)

            for header_param in HeaderInformation.__dict__.get("__annotations__"):
                try:
                    header_value = header.find(header_param).text
                except:
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
                    except:
                        item_value = None
                    item_parameter = {item_param: item_value}
                    item_params_dict.update(item_parameter)
                line_item = LineItemDetail(**item_params_dict)
                row = Order(HeaderInformation=header_info, LineItemDetail=line_item)
                my_dict = row.dict()
                final_df = final_df.append(
                    pd.json_normalize(my_dict, max_level=2), ignore_index=True
                )
    return final_df


class Epicor(Source):
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
    ):
        """
        Class to connect to Epicor API and download results.

        Args:
            base_url (str, required): Base url to Epicor Orders.
            filters_xml (str, required): Filters in form of XML. The date filter is required.
            credentials (Dict[str, Any], optional): Credentials to connect with Epicor API containing host, port, username and password.
                Defaults to None.
            config_key (str, optional): Credential key to dictionary where details are stored.
            start_date_field (str, optional) The name of filters field containing start date. Defaults to "BegInvoiceDate".
            end_date_field (str, optional) The name of filters field containing end date. Defaults to "EndInvoiceDate".
        """
        DEFAULT_CREDENTIALS = local_config.get(config_key)
        credentials = credentials or DEFAULT_CREDENTIALS

        required_credentials = ["host", "port", "username", "password"]
        if any([cred_key not in credentials for cred_key in required_credentials]):
            not_found = [c for c in required_credentials if c not in credentials]
            raise CredentialError(f"Missing credential(s): '{not_found}'.")

        self.credentials = credentials
        self.config_key = config_key
        self.base_url = base_url
        self.filters_xml = filters_xml
        self.start_date_field = start_date_field
        self.end_date_field = end_date_field

        super().__init__(*args, credentials=credentials, **kwargs)

    def generate_token(self) -> str:
        "Function to generate API access token that is valid for 24 hours"

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
        token = root.find("AccessToken").text
        return token

    def generate_url(self) -> str:
        "Function to generate url to download data"

        return (
            "http://"
            + self.credentials["host"]
            + ":"
            + str(self.credentials["port"])
            + self.base_url
        )

    def validate_filter(self) -> None:
        "Function checking if user had specified date range filters."

        root = ET.fromstring(self.filters_xml)
        for child in root:
            for subchild in child:
                if (
                    subchild.tag == self.start_date_field
                    or subchild.tag == self.end_date_field
                ) and subchild.text == None:
                    raise DataRangeError(
                        "Too much data. Please provide a date range filter."
                    )

    def get_xml_response(self):
        "Function for getting response from Epicor API"
        self.validate_filter()
        payload = self.filters_xml
        url = self.generate_url()
        headers = {
            "Content-Type": "application/xml",
            "Authorization": "Bearer " + self.generate_token(),
        }
        response = handle_api_response(
            url=url, headers=headers, body=payload, method="POST"
        )
        return response

    def to_df(self):
        "Function for creating pandas DataFrame from Epicor API response"
        data = self.get_xml_response()
        df = parse_orders_xml(data)
        return df
