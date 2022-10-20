from viadot.sources import ExchangeRates
import pandas as pd


TEST_DATA = {
    "currencies": [
        {
            "Date": "2022-10-09",
            "Base": "PLN",
            "USD": 0.200306,
            "EUR": 0.205766,
            "GBP": 0.180915,
            "CHF": 0.199232,
            "PLN": 1,
            "DKK": 1.530656,
        },
        {
            "Date": "2022-10-10",
            "Base": "PLN",
            "USD": 0.19987,
            "EUR": 0.205845,
            "GBP": 0.180461,
            "CHF": 0.199793,
            "PLN": 1,
            "DKK": 1.531165,
        },
        {
            "Date": "2022-10-11",
            "Base": "PLN",
            "USD": 0.199906,
            "EUR": 0.206092,
            "GBP": 0.182281,
            "CHF": 0.199446,
            "PLN": 1,
            "DKK": 1.53301,
        },
    ]
}

TEST_DF = pd.json_normalize(TEST_DATA["currencies"])


def test_credentials():
    source = ExchangeRates()
    assert source.credentials is not None


def test_to_json_values():
    source = ExchangeRates(
        currency="PLN",
        start_date="2022-10-09",
        end_date="2022-10-11",
        symbols=["USD", "EUR", "GBP", "CHF", "PLN", "DKK"],
    )
    expected_value = TEST_DATA.items()
    retrieved_value = source.to_json().items()

    assert retrieved_value == expected_value


def test_to_df_values():
    source = ExchangeRates(
        currency="PLN",
        start_date="2022-10-09",
        end_date="2022-10-11",
        symbols=["USD", "EUR", "GBP", "CHF", "PLN", "DKK"],
    )
    expected_value = TEST_DF
    retrieved_value = source.to_df()
    retrieved_value.drop(["_viadot_downloaded_at_utc"], axis=1, inplace=True)
    assert retrieved_value.iloc[0].equals(expected_value.iloc[0])


def test_get_columns():
    source = ExchangeRates(
        symbols=["USD", "EUR", "GBP", "CZK", "SEK", "NOK", "ISK"],
    )
    expected_columns = ["Date", "Base", "USD", "EUR", "GBP", "CZK", "SEK", "NOK", "ISK"]
    retrieved_columns = source.get_columns()
    assert retrieved_columns == expected_columns
