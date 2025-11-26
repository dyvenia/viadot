"""Source for connecting to Epicor Prelude API."""

from typing import Any, Literal
from xml.etree.ElementTree import Element as ElementT

from defusedxml import ElementTree as ElementDE
import pandas as pd
from pydantic import BaseModel
import requests

from viadot.config import get_source_credentials
from viadot.exceptions import DataRangeError, ValidationError
from viadot.sources.base import Source
from viadot.utils import add_viadot_metadata_columns, handle_api_response


def parse_orders_xml(response: requests.models.Response) -> pd.DataFrame:
    """Parse XML order data into a pandas DataFrame.

    Args:
        response (requests.models.Response): Response from Epicor API.

    Returns:
        pd.DataFrame: xml data in a form of pandas DataFrame.
    """
    root = ElementDE.fromstring(response.text)
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


def parse_customer_xml(response: requests.models.Response) -> pd.DataFrame:
    """Parse XML customer data into a pandas DataFrame.

    Args:
        response (requests.models.Response): Response from Epicor API.

    Returns:
        pd.DataFrame: xml data in a form of pandas DataFrame.
    """
    xml_str = response.text
    root = ElementDE.fromstring(xml_str)

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


def flatten_element(elem: ElementT, prefix: str = "") -> dict[str, str]:
    """Flatten element tree into a dict.

    Args:
        elem (str): XML element to flatten.
        prefix: Prefix for building dotted tag paths.

    Returns:
        dict[str, str]: Mapping of dotted tag paths to leaf text values.
    """
    if elem is None:
        return {}

    data = {}
    for child in elem:
        tag = f"{prefix}{child.tag}"

        if len(child):
            nested = flatten_element(child, prefix=tag + ".")
            data.update(nested)
        else:
            data[tag] = child.text

    return data


class EpicorCredentials(BaseModel):
    host: str
    port: int = 443
    username: str
    password: str


class Epicor(Source):
    """Source for connecting to Epicor Prelude API."""

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
        root = ElementDE.fromstring(response.text)
        return root.find("AccessToken").text

    def validate_filter(self, filters_xml: str) -> None:
        """Validate that the XML filter contains required date fields.

        Args:
            filters_xml: XML string with filter parameters.

        Raises:
            DataRangeError: If start or end date is missing.

        """
        root = ElementDE.fromstring(filters_xml)
        for child in root:
            for subchild in child:
                if (
                    subchild.tag in (self.start_date_field, self.end_date_field)
                ) and subchild.text is None:
                    msg = "Too much data. Please provide a date range filter."
                    raise DataRangeError(msg)

    def get_xml_response(self, filters_xml: str) -> requests.models.Response:
        """Send the XML filter request to the Epicor API and return the response.

        Args:
            filters_xml: XML string with filter parameters.

        Returns:
            Response: HTTP response from the Epicor API.
        """
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

    @add_viadot_metadata_columns
    def to_df(
        self,
        filters_xml: str,
        if_empty: Literal["warn", "skip", "fail"] = "warn",
    ) -> pd.DataFrame:
        """Convert Epicor API XML response to pandas DataFrame.

        Args:
            filters_xml (str): XML with query filters sent to the API.
            if_empty (Literal["warn", "skip", "fail"], optional):
                Behavior when the parsed DataFrame is empty. Defaults to "warn".

        Returns:
            pd.DataFrame: Parsed data returned by the appropriate XML parser.

        Raises:
            ValidationError: If no parser is available for the given view.

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
