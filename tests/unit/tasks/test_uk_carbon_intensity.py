import os

import openpyxl
import pytest
from openpyxl import load_workbook

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


# def test_uk_carbon_intensity_to_csv(ukci_task):
#     ukci_task.run(path=TEST_FILE_PATH)
#     if_exist = os.path.isfile(TEST_FILE_PATH)
#     assert if_exist == True


# def test_uk_carbon_intensity_to_excel(ukci_task_excel):
#     ukci_task_excel.run(path=TEST_FILE_PATH_EXCEL)
#     if_exist = os.path.isfile(TEST_FILE_PATH_EXCEL)
#     assert if_exist == True


# def test_uk_carbon_intensity_to_excel_contain(ukci_task_excel):
#     ukci_task_excel.run(path=TEST_FILE_PATH_EXCEL)
#     excel_file = load_workbook(TEST_FILE_PATH_EXCEL)
#     value = excel_file["A1"].value
#     assert value == "from"
