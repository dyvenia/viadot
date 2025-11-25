"""Source for connecting to Epicor Prelude API."""

from typing import Any, Literal
import xml.etree.ElementTree as ET

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


def parse_orders_xml(response):
    root = ET.fromstring(response.text)
    rows = []

    for child in root:
        header = child.find("HeaderInformation")
        header_data = flatten_element(header, prefix="HeaderInformation.")

        line_items_parent = child.find("LineItemDetails")
        line_items = (
            line_items_parent.findall("LineItemDetail")
            if line_items_parent is not None
            else []
        )

        if not line_items:
            rows.append(header_data)
            continue

        for item in line_items:
            item_data = flatten_element(item, prefix="LineItemDetail.")
            row = {**header_data, **item_data}
            rows.append(row)

    return pd.DataFrame(rows)


def flatten_element(elem, prefix=""):
    """Flatten element tree into a dict."""
    if elem is None:
        return {}

    data = {}
    for child in elem:
        tag = f"{prefix}{child.tag}"

        if len(child):  # ma pod-elementy
            nested = flatten_element(child, prefix=tag + ".")
            data.update(nested)
        else:
            data[tag] = child.text

    return data


def parse_customer_xml(response):
    xml_str = response.text
    root = ET.fromstring(xml_str)

    rows = []

    for cust in root.findall(".//Customer"):
        customer_fields = {}
        for child in cust:
            if child.tag != "ShipTos" and len(child) == 0:
                customer_fields[child.tag] = child.text

        shiptos_parent = cust.find("ShipTos")
        shiptos = (
            shiptos_parent.findall("ShipToAddress")
            if shiptos_parent is not None
            else []
        )

        if shiptos:
            for st in shiptos:
                row = customer_fields.copy()

                # flatten shipto fields
                for elem in st:
                    row[f"ShipToAddress.{elem.tag}"] = elem.text

                rows.append(row)

        else:
            # no shiptos: return only customer flat row
            rows.append(customer_fields)

    return pd.DataFrame(rows)


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

    def to_df(
        self,
        filters_xml: str,
        if_empty: Literal["warn", "skip", "fail"] = "warn",
    ) -> pd.DataFrame:
        """Function for creating pandas DataFrame from Epicor API response.

        Returns:
            pd.DataFrame: Output DataFrame.
        """
        data = self.get_xml_response(filters_xml)

        if (
            "ORDER.HISTORY.DETAIL.QUERY" in self.base_url
            or "ORDER.DETAIL.PROD.QUERY" in self.base_url
            or "BOOKINGS.DETAIL.QUERY" in self.base_url
        ):
            df = parse_orders_xml(data)
        elif "CUSTOMER.QUERY" in self.base_url:
            df = parse_customer_xml(data)
        else:
            msg = f"Parser for selected view {self.base_url} is not available"
            raise ValidationError(msg)
        if df.empty:
            self._handle_if_empty(if_empty)
        return df
