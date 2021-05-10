import datetime
import os

import numpy
import pyarrow
import pytest

from viadot.sources.uk_carbon_intensity import UKCarbonIntensity

LOCAL_TESTS_PATH = "/home/viadot/tests"
TEST_FILE_1 = os.path.join(LOCAL_TESTS_PATH, "tests_out.csv")
TEST_FILE_2 = os.path.join(LOCAL_TESTS_PATH, "testfile_stats.csv")


@pytest.fixture(scope="session")
def carbon():
    carbon = UKCarbonIntensity()
    yield carbon
    os.remove(TEST_FILE_1)
    os.remove(TEST_FILE_2)


def test_to_json(carbon):
    carbon.query("/intensity")
    data = carbon.to_json()
    assert "data" in data


def test_to_df(carbon):
    carbon.query("/intensity")
    df = carbon.to_df()
    assert type(df["actual"][0]) == numpy.int64


def test_to_arrow(carbon):
    carbon.query("/intensity")
    table = carbon.to_arrow()
    assert type(table) == pyarrow.lib.Table


def test_to_csv(carbon):
    carbon.query("/intensity")
    carbon.to_csv(TEST_FILE_1)
    assert os.path.isfile(TEST_FILE_1) == True


def test_stats_to_csv(carbon):
    now = datetime.datetime.now()
    for i in range(10):
        from_delta = datetime.timedelta(days=i + 1)
        to_delta = datetime.timedelta(days=i)
        to = now - to_delta
        from_ = now - from_delta
        carbon.query(f"/intensity/stats/{from_.isoformat()}/{to.isoformat()}")
        carbon.to_csv(TEST_FILE_2, if_exists="append")
    assert os.path.isfile(TEST_FILE_2) == True
