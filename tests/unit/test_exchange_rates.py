from ast import Not
from multiprocessing.context import assert_spawning
from viadot.sources import ExchangeRates
from viadot.config import local_config
import pandas as pd


def test_credentials():
    source = ExchangeRates()
    assert source.credentials is not None


def test_value_after_connetion_to_json():
    source = ExchangeRates()
    assert type(source.to_json()) is dict


"""
def test_value_after_connetion_to_df():
    source = ExchangeRates()
    assert type(source.to_df()) is pd.DataFrame



def test_value_after_connetion_to_df():
    source = ExchangeRates()
    assert source.to_df().isna().any()
"""

# def test_to_df():
#     pass
