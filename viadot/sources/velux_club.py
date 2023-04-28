import json
import urllib
from copy import deepcopy
from typing import Any, Dict, List
import requests

import numpy as np
import pandas as pd


from ..config import local_config
from ..exceptions import CredentialError
from ..utils import handle_api_response
from .base import Source


class VeluxClub(Source):

    """
    A class implementing the Velux Club API.

    Documentation for this API is located at: https://api.club.velux.com/api/v1/datalake/
    There are 4 endpoints where to get the data!

    Parameters
    ----------
    query_params : Dict[str, Any], optional
        The parameters to pass to the GET query.
        See https://supermetrics.com/docs/product-api-get-data/ for full specification,
        by default None
    """

    API_URL = "https://api.club.velux.com/api/v1/datalake/"

    def __init__(self, *args, source, credentials: Dict[str, Any] = None, from_date: str = '', to_date: str  = '', **kwargs):
        DEFAULT_CREDENTIALS = local_config.get("VELUX_CLUB")
        credentials = kwargs.pop("credentials", DEFAULT_CREDENTIALS)
        if credentials is None:
            raise CredentialError("Missing credentials.")
        
        self.headers = {
            "Authorization": "Bearer " + credentials["TOKEN"],
            "Content-Type": "application/json",
        }
        self.source = source
        self.from_date = from_date
        self.to_date = to_date

        super().__init__(*args, credentials=credentials, **kwargs)
        

    def get_api_body(
        self, region="null"
    ) -> Dict:  ## Returns the response 
        if self.source in ["jobs", "product", "company"]:
            # check if date filter was passed!
            if self.from_date == "" or self.to_date == "":
                return "Introduce a 'FROM Date' and 'To Date'"
            url = (
                f"{self.API_URL}{self.source}?from={self.from_date}&to={self.to_date}&region&limit=100"
            )
        elif self.source in "survey":
            url = f"{self.API_URL}{self.source}?language=en&type=question"
        else:
            return "pick one these sources: jobs, product, company, survey"

        headers = self.headers

        response = requests.request("GET", url, headers=headers)

        return response

    def to_df(self, response) -> pd.DataFrame:
        response = response.json()
        
        if isinstance(response, dict):
            keys_list = list(response.keys())
        elif isinstance(response, list):
            keys_list = list(response[0].keys())
        else:
            keys_list = []

        

        if "data" in keys_list:
            # first page content
            df = pd.DataFrame(response["data"])
            # next pages
            while response["next_page_url"] != None:
                url = f"{response['next_page_url']}&from={self.from_date}&to={self.to_date}&region&limit=100"
                r = requests.request("GET", url, headers=self.headers)
                response = r.json()
                df_page = pd.DataFrame(response["data"])
                if self.source == 'product':
                    df_page = df_page.T
                
                df = df.append(df_page, ignore_index=True)

        else:
            df = pd.DataFrame(response)
        
        return df

    def to_parquet(self):
        return


    def print_df(df: pd.DataFrame, service_name: str):
        print(f"{service_name} Dataframe Columns")
        print(list(df.columns))
        print(f"{service_name} Number of Columns")
        print(len(list(df.columns)))
        print(f"{service_name} Dataframe Number Of Samples")
        print(str(len(df.index)))
