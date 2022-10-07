from urllib import response
from ..config import local_config
from .base import Source
import pandas as pd
import requests
import json
from typing import Dict, Any


class ExchangeRates(Source):
    """_summary_

    Args:
        Source (_type_): retrieves cridentials from the Source class
    """

    def __init__(self, *args, credentials: Dict[str, Any] = None, **kwargs):
        credentials = credentials or local_config.get("EXCHANGE_RATES")
        super().__init__(*args, credentials=credentials, **kwargs)

    def to_json(self) -> dict:
        url = self.credentials["url"]
        headers = {"apikey": self.credentials["apikey"]}
        payload = {
            "start_date": (datetime.today() - timedelta(days=6)).strftime("%Y-%m-%d"),
            "end_date": datetime.today().strftime("%Y-%m-%d"),
            "base": "USD",
            "symbols": "USD,EUR,GBP,CHF,PLN,DKK,COP,CZK,SEK,NOK,ISK",
        }
        response = requests.request(
            "GET", self.credentials["url"], headers=headers, data={}, params=payload
        )

        return json.loads(response.text)

    def to_df(self) -> pd.DataFrame:
        df = pd.DataFrame(self.to_json())
        df2 = pd.json_normalize(df["rates"])
        df.reset_index(inplace=True)
        df = df[["index", "base"]].join(df2)

        return df
