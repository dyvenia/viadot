"""A connector for Cloud For Customers API."""

from copy import deepcopy
import re
from typing import Any, Literal
from urllib.parse import urljoin

import pandas as pd
from pydantic import BaseModel, SecretStr, root_validator
import requests

from viadot.config import get_source_credentials
from viadot.exceptions import CredentialError
from viadot.sources.base import Source
from viadot.utils import handle_api_response, validate


class CloudForCustomersCredentials(BaseModel):
    """Cloud for Customers connector credentials validator.

    Validate the credentials.

    Methods:
        is_configured: main method to validate.
    """

    username: str  # eg. username@{tenant_name}.com
    password: SecretStr
    url: str | None = None  # The URL to extract records from.
    report_url: str | None = None  # The URL of a prepared report.

    @classmethod
    @root_validator(pre=True)
    def is_configured(cls, credentials: dict) -> dict:
        """Validate Credentials.

        Args:
            credentials (dict): dictionary with user and password.

        Returns:
            credentials (dict): dictionary with user and password.
        """
        username = credentials.get("username")
        password = credentials.get("password")

        if not (username and password):
            msg = "`username` and `password` credentials are required."
            raise CredentialError(msg)

        return credentials


class CloudForCustomers(Source):
    """Cloud for Customers connector to fetch OData source.

    Args:
        url (str, optional): The URL to the C4C API. For example,
            'https://myNNNNNN.crm.ondemand.com/c4c/v1/'.
        endpoint (str, optional): The API endpoint.
        report_url (str, optional): The URL of a prepared report.
        filter_params (Dict[str, Any], optional): Filtering parameters passed to the
            request. E.g {"$filter": "AccountID eq '1234'"}. More info on:
            https://userapps.support.sap.com/sap/support/knowledge/en/2330688
        credentials (CloudForCustomersCredentials, optional): Cloud for Customers
            credentials.
        config_key (str, optional): The key in the viadot config holding relevant
            credentials.
    """

    DEFAULT_PARAMS = {"$format": "json"}  # noqa: RUF012

    def __init__(
        self,
        *args,
        url: str | None = None,
        endpoint: str | None = None,
        report_url: str | None = None,
        filter_params: dict[str, Any] | None = None,
        credentials: CloudForCustomersCredentials | None = None,
        config_key: str | None = None,
        **kwargs,
    ):
        """Initialize the class with the provided parameters.

        Args:
            *args: Variable length argument list.
            url (str, optional): The base URL for the service.
            endpoint (str, optional): The specific endpoint for the service.
            report_url (str, optional): The URL for the report.
            filter_params (Dict[str, Any], optional): Parameters to filter the report
                data.
            credentials (CloudForCustomersCredentials, optional): Credentials required
                for authentication.
            config_key (Optional[str], optional): A key to retrieve specific
                configuration settings.
            **kwargs: Arbitrary keyword arguments.
        """
        # Credentials logic
        raw_creds = credentials or get_source_credentials(config_key) or {}
        validated_creds = dict(
            CloudForCustomersCredentials(**raw_creds)
        )  # validate the credentials
        super().__init__(*args, credentials=validated_creds, **kwargs)

        self.url = url or self.credentials.get("url")
        self.report_url = report_url or self.credentials.get("report_url")

        self.is_report = bool(self.report_url)
        self.endpoint = endpoint

        if self.url:
            self.full_url = urljoin(self.url, self.endpoint)
        if filter_params:
            filter_params_merged = self.DEFAULT_PARAMS.copy()
            filter_params_merged.update(filter_params)

            self.filter_params = filter_params_merged
        else:
            self.filter_params = self.DEFAULT_PARAMS

    @staticmethod
    def create_metadata_url(url: str) -> str:
        """Create URL to fetch metadata from.

        Args:
            url (str): The URL to transform to metadata URL.

        Returns:
            meta_url (str): The URL to fetch metadata from.
        """
        start = url.split(".svc")[0]
        url_raw = url.split("?")[0]
        end = url_raw.split("/")[-1]
        return start + ".svc/$metadata?entityset=" + end

    def _extract_records_from_report_url(self, report_url: str) -> list[dict[str, Any]]:
        """Fetch report_url to extract records.

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

    def _extract_records_from_url(self, url: str) -> list[dict[str, Any]]:
        """Fetch URL to extract records.

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

            # prevents concatenation of previous urls with filter_params with the same
            # filter_params
            tmp_filter_params = None
            tmp_full_url = url

            if hasattr(new_records, "__iter__"):
                records.extend(new_records)
        return records

    def extract_records(
        self, url: str | None = None, report_url: str | None = None
    ) -> list[dict[str, Any]]:
        """Download records from `url` or `report_url` if present.

        Returns:
            records (List[Dict[str, Any]]): The records extracted from URL.
        """
        if self.is_report:
            return self._extract_records_from_report_url(report_url=report_url)
        full_url = urljoin(url, self.endpoint) if url else self.full_url
        return self._extract_records_from_url(url=full_url)

    def get_entities(
        self, dirty_json: dict[str, Any], url: str
    ) -> list[dict[str, Any]]:
        """Extract entities from request.json().

        Entities represent objects that store information. More info on:
        https://help.sap.com/docs/EAD_HANA/0e60f05842fd41078917822867220c78/
        0bd1db568fa546d6823d4c19a6b609ab.html

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
                if key not in ["__metadata", "Photo", "", "Picture"] and "{" not in str(
                    object_of_interest
                ):
                    new_key = column_maper_dict.get(key)
                    if new_key:
                        new_entity[new_key] = object_of_interest
                    else:
                        new_entity[key] = object_of_interest
            entities.append(new_entity)
        return entities

    def get_property_to_sap_label_dict(self, url: str | None = None) -> dict[str, str]:
        """Create Dict that maps Property Name to value of SAP label.

           Property: Properties define the characteristics of the data.
           SAP label: Labels are used for identification and for provision of content
           information.

        Args:
            url (str, optional): The URL to fetch metadata from.

        Returns:
            Dict[str, str]: Property Name to value of SAP label.
        """
        column_mapping = {}
        if url:
            username = self.credentials.get("username")
            password = self.credentials.get("password")
            response = requests.get(
                url,
                auth=(username, password.get_secret_value()),
                timeout=(3.05, 60 * 5),
            )
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
        filter_params: dict[str, Any] | None = None,
        timeout: tuple = (3.05, 60 * 30),
    ) -> requests.models.Response:
        """Handle requests.

        Args:
            url (str): The url to request to.
            filter_params (Dict[str, Any], optional): Additional parameters like filter,
                used in case of normal url.
            timeout (tuple, optional): The request time-out. Default is (3.05, 60 * 30).

        Returns:
            requests.models.Response.
        """
        username = self.credentials.get("username")
        password = self.credentials.get("password")
        return handle_api_response(
            url=url,
            params=filter_params,
            auth=(username, password.get_secret_value()),
            timeout=timeout,
        )

    def to_df(
        self,
        if_empty: Literal["warn", "skip", "fail"] = "warn",
        **kwargs,
    ) -> pd.DataFrame:
        """Download a table or report into a pandas DataFrame.

        Args:
            kwargs: The parameters to pass to DataFrame constructor.

        Returns:
            df (pandas.DataFrame): DataFrame containing the records.
        """
        # Your implementation here
        if if_empty == "warn":
            self.logger.info("Warning: DataFrame is empty.")
        elif if_empty == "skip":
            self.logger.info("Skipping due to empty DataFrame.")
        elif if_empty == "fail":
            self.logger.info("Failing due to empty DataFrame.")
        else:
            msg = "Invalid value for if_empty parameter."
            raise ValueError(msg)

        url: str = kwargs.get("url", "")
        fields: list[str] = kwargs.get("fields", [])
        dtype: dict[str, Any] = kwargs.get("dtype", {})
        tests: dict[str, Any] = kwargs.get("tests", {})

        url = url or self.url
        records = self.extract_records(url=url, report_url=self.report_url)
        df = pd.DataFrame(data=records, **kwargs)

        if dtype:
            df = df.astype(dtype)

        if fields:
            return df[fields]

        if tests:
            validate(df=df, tests=tests)

        return df
