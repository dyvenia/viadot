import os
import numpy
import pytest

from viadot.sources import CloudForCustomers
from viadot.config import local_config

LOCAL_TESTS_PATH = "/home/viadot/tests"
TEST_FILE_1 = os.path.join(LOCAL_TESTS_PATH, "tests_out.csv")


@pytest.fixture(scope="session")
def cloud_for_customers():
    url = "http://services.odata.org/V2/Northwind/Northwind.svc/"
    endpoint = "Employees"
    cloud_for_customers = CloudForCustomers(url=url, endpoint=endpoint)
    yield cloud_for_customers
    os.remove(TEST_FILE_1)


def test_to_json(cloud_for_customers):
    data = cloud_for_customers.to_json(fields=["EmployeeID", "FirstName", "LastName"])
    assert "EmployeeID" in data


def test_to_df(cloud_for_customers):
    df = cloud_for_customers.to_df(fields=["EmployeeID", "FirstName", "LastName"])
    assert type(df["EmployeeID"][0]) == numpy.int64


def test_csv(cloud_for_customers):
    csv = cloud_for_customers.to_csv(
        path=TEST_FILE_1, fields=["EmployeeID", "FirstName", "LastName"]
    )
    assert os.path.isfile(TEST_FILE_1) == True


def test_connections():
    credentials = local_config.get("CLOUD_FOR_CUSTOMERS")
    url = "https://my336539.crm.ondemand.com/sap/c4c/odata/v1/c4codataapi/"
    endpoint = "ServiceRequestCollection"
    c4c = CloudForCustomers(
        url=url,
        endpoint=endpoint,
        username=credentials["username"],
        password=credentials["password"],
    )
    df = c4c.to_df(
        fields=["ProductRecipientPartyName", "CreationDateTime", "CreatedBy"]
    )
    assert df.empty == False
