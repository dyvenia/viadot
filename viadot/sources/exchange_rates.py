from urllib import response
from ..config import local_config
from .base import Source
import pandas as pd
import requests
import json
from typing import Dict, Any


class ExchangeRates(Source):
    def __init__(self, *args, credentials: Dict[str, Any] = None, **kwargs):
        credentials = credentials or local_config.get("EXCHANGE_RATES")
        super().__init__(*args, credentials=credentials, **kwargs)

    def to_json(self) -> dict:
        url = self.credentials["url"]
        headers = {"apikey": self.credentials["apikey"]}
        payload = {
            "start_date": "2022-10-05",
            "end_date": "2022-10-06",
            "base": "PLN",
            "symbols": "EUR,GBP,CHF,PLN,DKK,COP,CZK,SEK,NOK,ISK",
        }
        response = requests.request(
            "GET", self.credentials["url"], headers=headers, data={}, params=payload
        )

        return json.loads(response.text)


""" To change
    def to_df(self) -> pd.DataFrame:
        df = pd.json_normalize(self.to_json())
        df.drop(df.columns[[0, 1]], axis=1, inplace=True)

        return df

    def get_rates(currency: str):
        return
        do_func_here()
        filter_data(currency)
"""
