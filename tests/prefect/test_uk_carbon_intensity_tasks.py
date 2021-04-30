import os

import pytest
from viadot.tasks.open_apis.uk_carbon_intensity import StatsToCSV

TEST_FILE_PATH = "/home/viadot/tests/uk_carbon_intensity_test.csv"


@pytest.fixture(scope="session")
def ukci_task():
    ukci_task = StatsToCSV()
    yield ukci_task
    os.remove(TEST_FILE_PATH)


def test_uk_carbon_intensity_to_csv(ukci_task):
    ukci_task.run(path=TEST_FILE_PATH)
