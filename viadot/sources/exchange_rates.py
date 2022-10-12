from ..config import local_config
from .base import Source
import pandas as pd
import requests
import json

from typing import Dict, Any, Literal, List
from datetime import datetime

Currency = Literal[
    "USD", "EUR", "GBP", "CHF", "PLN", "DKK", "COP", "CZK", "SEK", "NOK", "ISK"
]


class ExchangeRates(Source):
    def __init__(
        self,
        currency: Currency = "USD",
        start_date: str = datetime.today().strftime("%Y-%m-%d"),
        end_date: str = datetime.today().strftime("%Y-%m-%d"),
        symbols=[
            "USD",
            "EUR",
            "GBP",
            "CHF",
            "PLN",
            "DKK",
            "COP",
            "CZK",
            "SEK",
            "NOK",
            "ISK",
        ],
        *args,
        credentials: Dict[str, Any] = None,
        **kwargs,
    ):
        """_summary_

        Args:
            currency (Currency, optional): _description_. Defaults to "USD".
            start_date (str, optional): _description_. Defaults to datetime.today().strftime("%Y-%m-%d").
            end_date (str, optional): _description_. Defaults to datetime.today().strftime("%Y-%m-%d").
            symbols (list, optional): _description_. Defaults to [ "USD", "EUR", "GBP", "CHF", "PLN", "DKK", "COP", "CZK", "SEK", "NOK", "ISK" ], Only ISO codes.
            credentials (Dict[str, Any], optional): _description_. Defaults to None.
        """

        credentials = credentials or local_config.get("EXCHANGE_RATES")
        super().__init__(*args, credentials=credentials, **kwargs)
        self.currency = currency
        self.start_date = start_date
        self.end_date = end_date
        self.symbols = symbols
        self._validate_symbols(self.symbols, self.currency)

    def _validate_symbols(self, symbols, currency):
        cur_list = [
            "USD",
            "EUR",
            "GBP",
            "CHF",
            "PLN",
            "DKK",
            "COP",
            "CZK",
            "SEK",
            "NOK",
            "ISK",
        ]

        if currency not in cur_list:
            raise ValueError(
                f"The specified currency does not exist or is unsupported: {currency}"
            )

        for i in symbols:
            if i not in cur_list:
                raise ValueError(
                    f"The specified currency list item does not exist or is not supported: {i}"
                )

    def APIconnection(self) -> Dict[str, Any]:
        url = self.credentials["url"]
        headers = {"apikey": self.credentials["apikey"]}
        payload = {
            "start_date": self.start_date,
            "end_date": self.end_date,
            "base": self.currency,
            "symbols": ",".join(self.symbols),
        }
        try:
            response = requests.request(
                "GET", self.credentials["url"], headers=headers, data={}, params=payload
            )
        except ConnectionError as e:
            print(e)

        return json.loads(response.text)

    def to_records(self) -> List[tuple]:

        data = self.APIconnection()
        records = []

        for j in data["rates"]:
            records.append(j)
            records.append(data["base"])

            for i in data["rates"][j]:
                records.append(data["rates"][j][i])

        records = [x for x in zip(*[iter(records)] * (2 + len(self.symbols)))]

        return records

    def get_columns(self) -> List[str]:

        columns = ["Date", "Base"] + self.symbols

        return columns

    def to_json(self) -> Dict[str, Any]:

        records = self.to_records()
        columns = self.get_columns()
        records = [dict(zip(columns, records[i])) for i in range(len(records))]
        json = {}
        json["currencies"] = records

        return json

    def to_df(self) -> pd.DataFrame:
        json = self.to_json()
        df = pd.json_normalize(json["currencies"])

        return df
