import json
from datetime import datetime
from typing import Any, Dict, List, Literal

import pandas as pd
import requests

from viadot.exceptions import CredentialError
from viadot.utils import add_viadot_metadata_columns, cleanup_df, validate

from ..config import get_source_credentials
from .base import Source

Currency = Literal[
    "USD", "EUR", "GBP", "CHF", "PLN", "DKK", "COP", "CZK", "SEK", "NOK", "ISK"
]


class ExchangeRates(Source):

    URL = "https://api.apilayer.com/exchangerates_data/timeseries"

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
        credentials: Dict[str, Any] = None,
        config_key: str = None,
        *args,
        **kwargs,
    ):
        """
        Class for pulling data from https://api.apilayer.com/exchangerates_data/timeseries

        Args:
            currency (Currency, optional): Base currency to which prices of searched currencies are related. Defaults to "USD".
            start_date (str, optional): Initial date for data search. Data range is start_date ->
                end_date, supported format 'yyyy-mm-dd'. Defaults to datetime.today().strftime("%Y-%m-%d").
            end_date (str, optional): See above. Defaults to datetime.today().strftime("%Y-%m-%d").
            symbols (list, optional): List of currencies for which exchange rates from base currency will be fetch.
                Defaults to [ "USD", "EUR", "GBP", "CHF", "PLN", "DKK", "COP", "CZK", "SEK", "NOK", "ISK" ], Only ISO codes.
            credentials (Dict[str, Any], optional): 'api_key'. Defaults to None.
            config_key (str, optional): The key in the viadot config holding relevant credentials.
        """

        credentials = credentials or get_source_credentials(config_key)
        if credentials is None:
            raise CredentialError("Please specify the credentials.")
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

    def get_data(self) -> Dict[str, Any]:
        headers = {"apikey": self.credentials["api_key"]}
        payload = {
            "start_date": self.start_date,
            "end_date": self.end_date,
            "base": self.currency,
            "symbols": ",".join(self.symbols),
        }
        try:
            response = requests.request(
                "GET", ExchangeRates.URL, headers=headers, params=payload
            )
        except ConnectionError as e:
            raise e

        return json.loads(response.text)

    def to_records(self) -> List[tuple]:

        data = self.get_data()
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

    @add_viadot_metadata_columns
    def to_df(
        self,
        tests: dict = None,
    ) -> pd.DataFrame:
        json = self.to_json()
        df = pd.json_normalize(json["currencies"])
        df_clean = cleanup_df(df)

        if tests:
            validate(df=df_clean, tests=tests)

        return df_clean
