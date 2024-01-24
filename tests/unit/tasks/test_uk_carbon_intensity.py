import os

import pytest


from viadot.tasks.open_apis.uk_carbon_intensity import StatsToCSV, StatsToExcel

TEST_FILE_PATH = "/home/viadot/tests/uk_carbon_intensity_test.csv"
TEST_FILE_PATH_EXCEL = "/home/viadot/tests/uk_carbon_intensity_test.xlsx"


@pytest.fixture(scope="session")
def ukci_task():
    ukci_task = StatsToCSV()
    yield ukci_task
    os.remove(TEST_FILE_PATH)


@pytest.fixture(scope="session")
def ukci_task_excel():
    ukci_task_excel = StatsToExcel()
    yield ukci_task_excel
    os.remove(TEST_FILE_PATH_EXCEL)
