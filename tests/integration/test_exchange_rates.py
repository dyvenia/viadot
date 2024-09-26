import pandas as pd
import pytest

from viadot.sources import ExchangeRates


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


@pytest.fixture(scope="session")
def exchange_rates():
    return ExchangeRates(
        currency="PLN",
        start_date="2022-10-09",
        end_date="2022-10-11",
        symbols=["USD", "EUR", "GBP", "CHF", "PLN", "DKK"],
        config_key="exchange_rates_dev",
    )


def test_to_json_values(exchange_rates):
    expected_value = TEST_DATA.items()
    retrieved_value = exchange_rates.to_json().items()

    assert retrieved_value == expected_value


def test_to_df_values(exchange_rates):
    expected_value = TEST_DF

    # Running the to_df function without the wrapper adding metadata columns
    origin_to_df = exchange_rates.to_df.__wrapped__
    retrieved_value = origin_to_df(exchange_rates)

    assert retrieved_value.equals(expected_value)


def test_get_columns(exchange_rates):
    expected_columns = ["Date", "Base", "USD", "EUR", "GBP", "CHF", "PLN", "DKK"]
    retrieved_columns = exchange_rates.get_columns()

    assert retrieved_columns == expected_columns
