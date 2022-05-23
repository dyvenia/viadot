import requests
from typing import Any, Dict
import xml.etree.ElementTree as ET

from .base import Source
from ..config import local_config
from ..exceptions import CredentialError, DataRangeError


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
            filters_xml (str, required): Filters in form of XML. The date filter is necessary.
            credentials (Dict[str, Any], optional): Credentials to connect with Epicor Api containing host, port, username and password.
                Defaults to None.
            config_key (str, optional): Credential key to dictionary where details are stored.
            start_date_field (str, optional) The name of filters filed containing start date. Defaults to "BegInvoiceDate".
            end_date_field (str, optional) The name of filters filed containing end date. Defaults to "EndInvoiceDate".
        """
        DEFAULT_CREDENTIALS = local_config.get(config_key)
        credentials = credentials or DEFAULT_CREDENTIALS

        if credentials is None:
            raise CredentialError("Credentials not found.")

        self.credentials = credentials
        self.config_key = config_key
        self.base_url = base_url
        self.filters_xml = filters_xml
        self.start_date_field = start_date_field
        self.end_date_field = end_date_field

        super().__init__(*args, credentials=credentials, **kwargs)

    def generate_token(self) -> str:
        "Function to generate API access token that last 24 hours"

        url = (
            "http://"
            + self.credentials["host"]
            + ":"
            + str(self.credentials["port"])
            + "/api/security/token/?username="
            + self.credentials["username"]
            + "&password="
            + self.credentials["password"]
        )

        payload = {}
        files = {}
        headers = {
            "Content-Type": "application/xml",
        }

        response = requests.request(
            "POST", url, headers=headers, data=payload, files=files
        )

        return response.text

    def generate_url(self) -> str:
        "Function to generate url to download data"

        return (
            "http://"
            + self.credentials["host"]
            + ":"
            + str(self.credentials["port"])
            + self.base_url
            + "?token="
            + str(self.generate_token())
        )

    def check_filter(self) -> None:
        "Function checking if user had specified date range filters."

        root = ET.fromstring(self.filters_xml)
        for child in root:
            for subchild in child:
                if (
                    subchild.tag == self.start_date_field
                    or subchild.tag == self.end_date_field
                ) and subchild.text == None:
                    raise DataRangeError(
                        "The data filter must be provided due to full data size."
                    )

    def get_xml_response(self):
        "Function for getting response from Epicor API"

        self.check_filter()
        payload = self.filters_xml
        url = self.generate_url()
        headers = {"Content-Type": "application/xml"}
        response = requests.request("POST", url, headers=headers, data=payload)

        return response
