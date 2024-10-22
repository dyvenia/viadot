"""Source for connecting to Business Core API."""

import json
from typing import Any, Literal

import pandas as pd
from pydantic import BaseModel, SecretStr

from viadot.config import get_source_credentials
from viadot.exceptions import APIError
from viadot.sources.base import Source
from viadot.utils import add_viadot_metadata_columns, handle_api_response


class BusinessCoreCredentials(BaseModel):
    """Business Core credentials.

    Uses simple authentication:
        - username: The user name to use.
        - password: The password to use.
    """

    username: str
    password: SecretStr


class BusinessCore(Source):
    """Business Core ERP API connector."""

    def __init__(
        self,
        url: str | None = None,
        filters: dict[str, Any] | None = None,
        credentials: dict[str, Any] | None = None,
        config_key: str = "BusinessCore",
        verify: bool = True,
        *args,
        **kwargs,
    ):
        """Create a BusinessCore connector instance.

        Args:
            url (str, optional): Base url to a view in Business Core API.
                Defaults to None.
            filters (dict[str, Any], optional): Filters in form of dictionary. Available
                filters: 'BucketCount', 'BucketNo', 'FromDate', 'ToDate'. Defaults to
                None.
            credentials (dict[str, Any], optional): Credentials stored in a dictionary.
                Required credentials: username, password. Defaults to None.
            config_key (str, optional): The key in the viadot config holding relevant
                credentials. Defaults to "BusinessCore".
            verify (bool, optional): Whether or not verify certificates while connecting
                to an API. Defaults to True.
        """
        raw_creds = credentials or get_source_credentials(config_key)
        validated_creds = dict(BusinessCoreCredentials(**raw_creds))

        self.url = url
        # API requires that filters are always specified
        if filters is None:
            self.filters = {
                "BucketCount": "&",
                "BucketNo": "&",
                "FromDate": "&",
                "ToDate": "&",
            }
        else:
            self.filters = self._clean_filters(filters)
        self.verify = verify

        super().__init__(*args, credentials=validated_creds, **kwargs)

    def generate_token(self) -> str:
        """Generate a token for the user.

        Returns:
            string: The token.
        """
        url = "https://api.businesscore.ae/api/user/Login"

        username = self.credentials.get("username")
        password = self.credentials.get("password").get_secret_value()
        payload = f"grant_type=password&username={username}&password={password}&scope="
        headers = {"Content-Type": "application/x-www-form-urlencoded"}
        response = handle_api_response(
            url=url,
            headers=headers,
            method="GET",
            data=payload,
            verify=self.verify,
        )

        return json.loads(response.text).get("access_token")

    @staticmethod
    def _clean_filters(filters: dict[str, str | None]) -> dict[str, str]:
        """Replace 'None' with '&' in a dictionary.

        Required for payload in 'x-www-form-urlencoded' from.

        Returns:
            dict[str, str]: Dictionary with filters prepared for further use.
        """
        return {key: ("&" if val is None else val) for key, val in filters.items()}

    def get_data(self) -> dict[str, Any]:
        """Obtain data from Business Core API.

        Returns:
            dict: Dictionary with data downloaded from Business Core API.
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
            error_message = f"View {view} currently not available."
            raise APIError(error_message)

        payload = (
            "BucketCount="
            + str(self.filters.get("BucketCount"))
            + "BucketNo="
            + str(self.filters.get("BucketNo"))
            + "FromDate="
            + str(self.filters.get("FromDate"))
            + "ToDate"
            + str(self.filters.get("ToDate"))
        )
        headers = {
            "Content-Type": "application/x-www-form-urlencoded",
            "Authorization": "Bearer " + self.generate_token(),
        }
        self.logger.info("Downloading the data...")
        response = handle_api_response(
            url=self.url,
            headers=headers,
            method="GET",
            data=payload,
            verify=self.verify,
        )
        self.logger.info("Data was downloaded successfully.")
        return json.loads(response.text).get("MasterDataList")

    @add_viadot_metadata_columns
    def to_df(self, if_empty: Literal["warn", "fail", "skip"] = "skip") -> pd.DataFrame:
        """Download data into a pandas DataFrame.

        Args:
            if_empty (Literal["warn", "fail", "skip"], optional): What to do if output
                DataFrame is empty. Defaults to "skip".

        Returns:
            pd.DataFrame: DataFrame with the data.

        Raises:
            APIError: When selected API view is not available.
        """
        data = self.get_data()
        df = pd.DataFrame.from_dict(data)
        self.logger.info(
            f"Data was successfully transformed into DataFrame: {len(df.columns)} columns and {len(df)} rows."
        )
        if df.empty:
            self._handle_if_empty(if_empty)

        return df
