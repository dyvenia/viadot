import os

import pytest

from viadot.sources import CloudForCustomers

TEST_FILE_1 = "tests_out.csv"


@pytest.fixture(scope="session")
def cloud_for_customers():
    url = "http://services.odata.org/V2/Northwind/Northwind.svc/"
    endpoint = "Employees"
    cloud_for_customers = CloudForCustomers(
        url=url, endpoint=endpoint, params={"$top": "2"}
    )
    yield cloud_for_customers
    os.remove(TEST_FILE_1)


def test_to_df(cloud_for_customers):
    df = cloud_for_customers.to_df(fields=["EmployeeID", "FirstName", "LastName"])
    assert not df.empty
    assert df.shape[1] == 3
    assert df.shape[0] == 2


def test_to_records(cloud_for_customers):
    data = cloud_for_customers.to_records()
    assert "EmployeeID" in data[0].keys()
    assert data != []
    assert len(data) == 2


def test_csv(cloud_for_customers):
    csv = cloud_for_customers.to_csv(path=TEST_FILE_1)
    assert os.path.isfile(TEST_FILE_1) == True
