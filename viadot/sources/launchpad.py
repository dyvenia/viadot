from copy import deepcopy
from typing import Any, Dict, List, Optional

import pandas as pd
import requests
from pydantic import BaseModel, SecretStr

from viadot.config import get_source_credentials
from viadot.exceptions import CredentialError
from viadot.sources.base import Source
from viadot.utils import handle_api_response


class LaunchpadCredentials(BaseModel):
    username: str  # eg. username@{tenant_name}.com
    password: SecretStr


class Launchpad(Source):
    """Launchpad connector to fetch Odata source.

    Args:
        url (str, optional): The URL to the Launchpad API, e.g. 'https://launchpad.myNNNNNN.com/sap/opu/odata/sap/'.
        filter_params (Dict[str, Any], optional): Filtering parameters passed to the request, e.g. {"$filter": "AccountID eq '1234'"}.
        credentials (LaunchpadCredentials, optional): Launchpad credentials.
        config_key (str, optional): The key in the viadot config holding relevant credentials.
    """

    DEFAULT_PARAMS = {"$format": "json"}

    def __init__(
        self,
        url: str = None,
        filter_params: Dict[str, Any] = None,
        credentials: LaunchpadCredentials = None,
        config_key: Optional[str] = None,
        *args,
        **kwargs,
    ):
        credentials = credentials or get_source_credentials(config_key)
        if credentials is None:
            raise CredentialError("Please specify the credentials.")
        super().__init__(*args, credentials=credentials, **kwargs)

        self.url = url

        if self.url is None:
            raise CredentialError("Please provide url.")

        if filter_params:
            filter_params_merged = self.DEFAULT_PARAMS.copy()
            filter_params_merged.update(filter_params)

            self.filter_params = filter_params_merged
        else:
            self.filter_params = self.DEFAULT_PARAMS

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

    def extract_records(self, url: str) -> List[Dict[str, Any]]:
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
        records = self.extract_records(url=self.url)
        df = pd.DataFrame(data=records, **kwargs)
        if dtype:
            df = df.astype(dtype)
        if fields:
            return df[fields]
        return df
