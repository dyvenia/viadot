import re
import requests
import pandas as pd

from copy import deepcopy
from typing import Any, Dict, List, Optional
from pydantic import BaseModel, SecretStr
from urllib.parse import urljoin
from viadot.exceptions import CredentialError

from ..utils import handle_api_response
from ..config import get_source_credentials
from .base import Source


class CloudForCustomersCredentials(BaseModel):
    username: str  # eg. username@{tenant_name}.com
    password: SecretStr


class CloudForCustomers(Source):
    """Cloud for Customers connector to fetch Odata source.

    Args:
        url (str, optional): The URL to the C4C API. E.g 'https://my396876543.jrt.ondemand.com/c4c/v1/'.
        endpoint (str, optional): The API endpoint.
        report_url (str, optional): The API URL in case of prepared report.
        filter_params (Dict[str, Any], optional): Filtering parameters passed to the request. E.g {"$filter": "AccountID eq '1234'"}.
        More info on:   https://userapps.support.sap.com/sap/support/knowledge/en/2330688
        credentials (CloudForCustomersCredentials, optional): Cloud for Customers credentials.
        config_key (str, optional): The key in the viadot config holding relevant credentials.
    """

    DEFAULT_PARAMS = {"$format": "json"}

    def __init__(
        self,
        url: str = None,
        endpoint: str = None,
        report_url: str = None,
        filter_params: Dict[str, Any] = None,
        credentials: CloudForCustomersCredentials = None,
        config_key: Optional[str] = None,
        *args,
        **kwargs,
    ):

        ## Credentials logic
        credentials = credentials or get_source_credentials(config_key)
        if credentials is None:
            raise CredentialError("Please specify the credentials.")
        CloudForCustomersCredentials(**credentials)  # validate the credentials schema
        super().__init__(*args, credentials=credentials, **kwargs)
        ## End Credentials logic

        self.url = url or self.credentials.get("url")
        self.report_url = report_url

        if self.url is None and report_url is None:
            raise CredentialError("One of: ('url', 'report_url') is required.")

        self.is_report = bool(report_url)
        self.query_endpoint = endpoint

        if filter_params:
            filter_params_merged = self.DEFAULT_PARAMS.copy()
            filter_params_merged.update(filter_params)

            self.filter_params = filter_params_merged
        else:
            self.filter_params = self.DEFAULT_PARAMS

        if self.url:
            self.full_url = urljoin(self.url, self.query_endpoint)

    @staticmethod
    def create_metadata_url(url: str) -> str:
        """Creates URL to fetch metadata from.

        Args:
            url (str): The URL to transform to metadata URL.

        Returns:
            meta_url (str): The URL to fetch metadata from.
        """
        start = url.split(".svc")[0]
        url_raw = url.split("?")[0]
        end = url_raw.split("/")[-1]
        meta_url = start + ".svc/$metadata?entityset=" + end
        return meta_url

    def _extract_records_from_report_url(self, report_url: str) -> List[Dict[str, Any]]:
        """Fetches report_url to extract records.

        Args:
            report_url (str): The url to extract records from.

        Returns:
            records (List[Dict[str, Any]]): The records extracted from report_url.
        """
        records = []
        while report_url:
            response = self.get_response(report_url, filter_params=self.filter_params)
            response_json = response.json()
            new_records = self.get_entities(response_json, report_url)
            records.extend(new_records)

            report_url = response_json["d"].get("__next")

        return records

    def _extract_records_from_url(self, url: str) -> List[Dict[str, Any]]:
        """Fetches URL to extract records.

        Args:
            url (str): The URL to extract records from.

        Returns:
            records (List[Dict[str, Any]]): The records extracted from URL.
        """
        tmp_full_url = deepcopy(url)
        tmp_filter_params = deepcopy(self.filter_params)
        records = []
        while url:
            response = self.get_response(tmp_full_url, filter_params=tmp_filter_params)
            response_json = response.json()
            if isinstance(response_json["d"], dict):
                # ODATA v2+ API
                new_records = response_json["d"].get("results")
                url = response_json["d"].get("__next", None)
            else:
                # ODATA v1
                new_records = response_json["d"]
                url = response_json.get("__next", None)

            # prevents concatenation of previous urls with filter_params with the same filter_params
            tmp_filter_params = None
            tmp_full_url = url

            if hasattr(new_records, "__iter__"):
                records.extend(new_records)
        return records

    def extract_records(self) -> List[Dict[str, Any]]:
        """Downloads records from `url` or `report_url` if present.

        Returns:
            records (List[Dict[str, Any]]): The records extracted from URL.
        """
        if self.is_report:
            return self._extract_records_from_report_url(url=self.report_url)
        else:
            return self._extract_records_from_url(url=self.full_url)

    def get_entities(
        self, dirty_json: Dict[str, Any], url: str
    ) -> List[Dict[str, Any]]:
        """Extracts entities from request.json(). Entities represents objects that store information.
           More info on: https://help.sap.com/docs/EAD_HANA/0e60f05842fd41078917822867220c78/0bd1db568fa546d6823d4c19a6b609ab.html

        Args:
            dirty_json (Dict[str, Any]): request.json() dict from response to API.
            url (str): The URL to fetch metadata from.

        Returns:
            entities (List[Dict[str, Any]]): list filled with entities.
        """

        metadata_url = self.create_metadata_url(url)
        column_maper_dict = self.get_property_to_sap_label_dict(metadata_url)
        entities = []
        for element in dirty_json["d"]["results"]:
            new_entity = {}
            for key, object_of_interest in element.items():
                if key not in ["__metadata", "Photo", "", "Picture"]:
                    if "{" not in str(object_of_interest):
                        new_key = column_maper_dict.get(key)
                        if new_key:
                            new_entity[new_key] = object_of_interest
                        else:
                            new_entity[key] = object_of_interest
            entities.append(new_entity)
        return entities

    def get_property_to_sap_label_dict(self, url: str = None) -> Dict[str, str]:
        """Creates Dict that maps Property Name to value of SAP label.
           Property: Properties define the characteristics of the data.
           SAP label: Labels are used for identification and for provision of content information.

        Args:
            url (str, optional): The URL to fetch metadata from.

        Returns:
            Dict[str, str]: Property Name to value of SAP label.
        """

        column_mapping = {}
        if url:
            username = self.credentials.get("username")
            pw = self.credentials.get("password")
            response = requests.get(url, auth=(username, pw))
            for sentence in response.text.split("/>"):
                result = re.search(
                    r'(?<=Name=")([^"]+).+(sap:label=")([^"]+)+', sentence
                )
                if result:
                    key = result.groups(0)[0]
                    val = result.groups(0)[2]
                    column_mapping[key] = val
        return column_mapping

    def get_response(
        self,
        url: str,
        filter_params: Dict[str, Any] = None,
        timeout: tuple = (3.05, 60 * 30),
    ) -> requests.models.Response:
        """Handles requests.

        Args:
            url (str): The url to request to.
            filter_params (Dict[str, Any], optional): Additional parameters like filter, used in case of normal url.
            timeout (tuple, optional): The request time-out. Default is (3.05, 60 * 30).

        Returns:
            requests.models.Response.
        """
        username = self.credentials.get("username")
        pw = self.credentials.get("password")
        response = handle_api_response(
            url=url,
            params=filter_params,
            auth=(username, pw),
            timeout=timeout,
        )
        return response

    def to_df(
        self,
        fields: List[str] = None,
        dtype: dict = None,
        **kwargs,
    ) -> pd.DataFrame:
        """Returns records in a pandas DataFrame.

        Args:
            fields (List[str], optional): List of fields to put in DataFrame.
            dtype (dict, optional): The dtypes to use in the DataFrame.
            kwargs: The parameters to pass to DataFrame constructor.

        Returns:
            df (pandas.DataFrmae): DataFrame containing all records.
        """
        records = self.extract_records()
        df = pd.DataFrame(data=records, **kwargs)
        if dtype:
            df = df.astype(dtype)
        if fields:
            return df[fields]
        return df
