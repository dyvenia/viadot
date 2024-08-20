"""Exchange Rates API connector."""

from datetime import datetime
import json
from typing import Any, Literal

import pandas as pd
import requests

from viadot.config import get_source_credentials
from viadot.exceptions import CredentialError
from viadot.sources.base import Source
from viadot.utils import add_viadot_metadata_columns, cleanup_df, validate


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
        symbols: list[str] | None = None,
        credentials: dict[str, Any] | None = None,
        config_key: str | None = None,
        *args,
        **kwargs,
    ):
        """Download data from https://api.apilayer.com/exchangerates_data/timeseries.

        Args:
            currency (Currency, optional): Base currency to which prices of searched
                currencies are related. Defaults to "USD".
            start_date (str, optional): Initial date for data search. Data range is
                start_date -> end_date, supported format 'yyyy-mm-dd'. Defaults to
                    datetime.today().strftime("%Y-%m-%d").
            end_date (str, optional): See above. Defaults to
                datetime.today().strftime("%Y-%m-%d").
            symbols (list, optional): List of ISO codes for which exchange rates from
                base currency will be fetched. Defaults to ["USD", "EUR", "GBP", "CHF",
                "PLN", "DKK", "COP", "CZK", "SEK", "NOK", "ISK" ].
            credentials (Dict[str, Any], optional): The credentials to use. Defaults to
                None.
            config_key (str, optional): The key in the viadot config holding relevant
                credentials.
        """
        credentials = credentials or get_source_credentials(config_key)
        if credentials is None:
            msg = "Please specify the credentials."
            raise CredentialError(msg)
        super().__init__(*args, credentials=credentials, **kwargs)

        if not symbols:
            symbols = [
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

        self.currency = currency
        self.start_date = start_date
        self.end_date = end_date
        self.symbols = symbols
        self._validate_symbols(self.symbols, self.currency)

    def _validate_symbols(self, symbols: list[str], currency: str):
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
            msg = f"The specified currency does not exist or is unsupported: {currency}"
            raise ValueError(msg)

        for i in symbols:
            if i not in cur_list:
                msg = f"The specified currency list item does not exist or is not supported: {i}"
                raise ValueError(msg)

    def get_data(self) -> dict[str, Any]:
        """Download data from the API.

        Returns:
            dict[str, Any]: The data from the API.
        """
        headers = {"apikey": self.credentials["api_key"]}
        payload = {
            "start_date": self.start_date,
            "end_date": self.end_date,
            "base": self.currency,
            "symbols": ",".join(self.symbols),
        }
        response = requests.request(
            "GET", ExchangeRates.URL, headers=headers, params=payload, timeout=(3, 10)
        )

        return json.loads(response.text)

    def to_records(self) -> list[tuple]:
        """Download data and convert it to a list of records.

        Returns:
            list[tuple]: The records of the data.
        """
        data = self.get_data()
        records = []

        for j in data["rates"]:
            records.append(j)
            records.append(data["base"])

            for i in data["rates"][j]:
                records.append(data["rates"][j][i])

        return list(zip(*[iter(records)] * (2 + len(self.symbols)), strict=False))

    def get_columns(self) -> list[str]:
        """Return the columns of the data.

        Returns:
            list[str]: The columns of the data.
        """
        return ["Date", "Base", *self.symbols]

    def to_json(self) -> dict[str, Any]:
        """Download data and convert it to a JSON.

        Returns:
            dict[str, Any]: The JSON with the data.
        """
        records = self.to_records()
        columns = self.get_columns()
        records = [
            dict(zip(columns, records[i], strict=False)) for i in range(len(records))
        ]
        json = {}
        json["currencies"] = records

        return json

    @add_viadot_metadata_columns
    def to_df(
        self,
        tests: dict | None = None,
    ) -> pd.DataFrame:
        """Download data and convert it to a pandas DataFrame.

        Args:
            tests (dict | None, optional): The tests specification. Defaults to None.

        Returns:
            pd.DataFrame: The pandas DataFrame with the data.
        """
        json = self.to_json()
        df = pd.json_normalize(json["currencies"])
        df_clean = cleanup_df(df)

        if tests:
            validate(df=df_clean, tests=tests)

        return df_clean
