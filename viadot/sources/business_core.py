import json
from typing import Any, Dict, Literal

import pandas as pd
from prefect.utilities import logging

from ..config import local_config
from ..exceptions import APIError, CredentialError
from ..utils import handle_api_response
from .base import Source

logger = logging.get_logger(__name__)


class BusinessCore(Source):
    """
    Source for getting data from Bussines Core ERP API.

    """

    def __init__(
        self,
        url: str = None,
        filters_dict: Dict[str, Any] = {
            "BucketCount": None,
            "BucketNo": None,
            "FromDate": None,
            "ToDate": None,
        },
        verify: bool = True,
        credentials: Dict[str, Any] = None,
        config_key: str = "BusinessCore",
        *args,
        **kwargs,
    ):
        """
        Creating an instance of BusinessCore source class.

        Args:
            url (str, optional): Base url to a view in Business Core API. Defaults to None.
            filters_dict (Dict[str, Any], optional): Filters in form of dictionary. Available filters: 'BucketCount',
                'BucketNo', 'FromDate', 'ToDate'.  Defaults to {"BucketCount": None,"BucketNo": None,"FromDate": None,
                "ToDate": None,}.
            verify (bool, optional): Whether or not verify certificates while connecting to an API. Defaults to True.
            credentials (Dict[str, Any], optional): Credentials stored in a dictionary. Required credentials: username,
                password. Defaults to None.
            config_key (str, optional): Credential key to dictionary where details are stored. Defaults to "BusinessCore".

        Raises:
            CredentialError: When credentials are not found.
        """
        DEFAULT_CREDENTIALS = local_config.get(config_key)
        credentials = credentials or DEFAULT_CREDENTIALS

        required_credentials = ["username", "password"]
        if any([cred_key not in credentials for cred_key in required_credentials]):
            not_found = [c for c in required_credentials if c not in credentials]
            raise CredentialError(f"Missing credential(s): '{not_found}'.")

        self.config_key = config_key
        self.url = url
        self.filters_dict = filters_dict
        self.verify = verify

        super().__init__(*args, credentials=credentials, **kwargs)

    def generate_token(self) -> str:
        """
        Function for generating Business Core API token based on username and password.

        Returns:
        string: Business Core API token.

        """
        url = "https://api.businesscore.ae/api/user/Login"

        payload = f'grant_type=password&username={self.credentials.get("username")}&password={self.credentials.get("password")}&scope='
        headers = {"Content-Type": "application/x-www-form-urlencoded"}
        response = handle_api_response(
            url=url, headers=headers, method="GET", body=payload, verify=self.verify
        )
        token = json.loads(response.text).get("access_token")
        self.token = token
        return token

    def clean_filters_dict(self) -> Dict:
        """
        Function for replacing 'None' with '&' in a dictionary. Needed for payload in 'x-www-form-urlencoded' from.

        Returns:
        Dict: Dictionary with filters prepared for further use.
        """
        return {
            key: ("&" if val is None else val) for key, val in self.filters_dict.items()
        }

    def get_data(self) -> Dict:
        """
        Function for obtaining data in dictionary format from Business Core API.

        Returns:
        Dict: Dictionary with data downloaded from Business Core API.
        """
        filters = self.clean_filters_dict()

        payload = (
            "BucketCount="
            + str(filters.get("BucketCount"))
            + "BucketNo="
            + str(filters.get("BucketNo"))
            + "FromDate="
            + str(filters.get("FromDate"))
            + "ToDate"
            + str(filters.get("ToDate"))
        )
        headers = {
            "Content-Type": "application/x-www-form-urlencoded",
            "Authorization": "Bearer " + self.generate_token(),
        }
        logger.info("Downloading the data...")
        response = handle_api_response(
            url=self.url,
            headers=headers,
            method="GET",
            body=payload,
            verify=self.verify,
        )
        logger.info("Data was downloaded successfully.")
        return json.loads(response.text)

    def to_df(self, if_empty: Literal["warn", "fail", "skip"] = "skip") -> pd.DataFrame:
        """
        Function for transforming data from dictionary to pd.DataFrame.

        Args:
            if_empty (Literal["warn", "fail", "skip"], optional): What to do if output DataFrame is empty. Defaults to "skip".

        Returns:
            pd.DataFrame: DataFrame with data downloaded from Business Core API view.

        Raises:
            APIError: When selected API view is not available.
        """
        view = self.url.split("/")[-1]

        if view not in [
            "GetCustomerData",
            "GetItemMaster",
            "GetPendingSalesOrderData",
            "GetSalesInvoiceData",
            "GetSalesReturnDetailData",
            "GetSalesOrderData",
            "GetSalesQuotationData",
        ]:
            raise APIError(f"View {view} currently not available.")

        data = self.get_data().get("MasterDataList")
        df = pd.DataFrame.from_dict(data)
        logger.info(
            f"Data was successfully transformed into DataFrame: {len(df.columns)} columns and {len(df)} rows."
        )
        if df.empty is True:
            self._handle_if_empty(if_empty)

        return df
