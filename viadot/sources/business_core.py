import pandas as pd
import json
from prefect.utilities import logging
from typing import Any, Dict

from ..exceptions import CredentialError, APIError
from .base import Source
from ..config import local_config
from ..utils import handle_api_response


logger = logging.get_logger(__name__)


class BusinessCore(Source):
    def __init__(
        self,
        url: str = None,
        filters_dict: Dict[str, Any] = {
            "BucketCount": None,
            "BucketNo": None,
            "FromDate": None,
            "ToDate": None,
        },
        credentials: Dict[str, Any] = None,
        config_key: str = "BusinessCore",
        *args,
        **kwargs,
    ):
        DEFAULT_CREDENTIALS = local_config.get(config_key)
        credentials = credentials or DEFAULT_CREDENTIALS

        required_credentials = ["username", "password"]
        if any([cred_key not in credentials for cred_key in required_credentials]):
            not_found = [c for c in required_credentials if c not in credentials]
            raise CredentialError(f"Missing credential(s): '{not_found}'.")

        self.credentials = credentials
        self.config_key = config_key
        self.url = url
        self.filters_dict = filters_dict

        super().__init__(*args, credentials=credentials, **kwargs)

    def generate_token(self) -> str:
        url = "https://api.businesscore.ae/api/user/Login"

        payload = f'grant_type=password&username={self.credentials.get("username")}&password={self.credentials.get("password")}&scope='
        headers = {"Content-Type": "application/x-www-form-urlencoded"}
        response = handle_api_response(
            url=url, headers=headers, method="GET", body=payload, verify=False
        )
        token = json.loads(response.text).get("access_token")
        self.token = token
        return token

    def clean_filters_dict(self) -> Dict:
        return {
            key: ("&" if val is None else val) for key, val in self.filters_dict.items()
        }

    def get_data(self) -> Dict:
        filters = self.clean_filters_dict()

        payload = (
            "BucketCount="
            + filters.get("BucketCount")
            + "BucketNo="
            + filters.get("BucketNo")
            + "FromDate="
            + filters.get("FromDate")
            + "ToDate"
            + filters.get("ToDate")
        )
        headers = {
            "Content-Type": "application/x-www-form-urlencoded",
            "Authorization": "Bearer " + self.generate_token(),
        }
        logger.info("Downloading the data...")
        response = handle_api_response(
            url=self.url, headers=headers, method="GET", body=payload, verify=False
        )
        logger.info("Data was downloaded successfully.")
        return json.loads(response.text)

    def to_df(self):
        view = self.url.split("/")[-1]
        if view not in ["GetCustomerData", "GetItemMaster", "GetPendingSalesOrderData"]:
            raise APIError(f"View {view} currently not available.")
        if view in ("GetCustomerData", "GetItemMaster"):
            data = self.get_data().get("MasterDataList")
            df = pd.DataFrame.from_dict(data)
            logger.info(
                f"Data was successfully transformed into DataFrame: {len(df.columns)} columns and {len(df)} rows."
            )
            return df
        if view == "GetPendingSalesOrderData":
            # todo waiting for schema
            raise APIError(f"View {view} currently not available.")
