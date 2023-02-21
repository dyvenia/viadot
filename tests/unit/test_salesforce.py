import pandas as pd
import pytest
from viadot.sources import Salesforce

OBJECT_NAME = "TestObject__c"
ROW_NAME = "Test_row_1"
UPSERTED_ROW = "Test_upser"


@pytest.fixture(scope="session", autouse=True)
def setting_up_test_environment():

    sf = Salesforce(config_key="sales-force")
    sf_api = sf.salesforce

    mdapi = sf_api.mdapi

    custom_object = mdapi.CustomObject(
        fullName=OBJECT_NAME,
        label="Test Object",
        pluralLabel="Custom Objects",
        nameField=mdapi.CustomField(label="Name", type=mdapi.FieldType("Text")),
        deploymentStatus=mdapi.DeploymentStatus("Deployed"),
        sharingModel=mdapi.SharingModel("ReadWrite"),
    )

    mdapi.CustomObject.create(custom_object)
    sf_api.TestObject__c.create({"Name": ROW_NAME})

    yield

    mdapi.CustomObject.delete(OBJECT_NAME)


def test_to_df_selected_table(setting_up_test_environment):

    sf = Salesforce(config_key="sales-force")
    df = sf.to_df(table=OBJECT_NAME)
    assert df["Name"][0] == ROW_NAME
    assert len(df.axes[1]) == 10


def test_to_df_selected_table_query(setting_up_test_environment):
    sf = Salesforce(config_key="sales-force")
    df = sf.to_df(query="SELECT FIELDS(STANDARD) FROM TestObject__c")
    assert df["Name"][0] == ROW_NAME
    assert len(df.axes[1]) == 10


def test_upsert_existing_row(setting_up_test_environment):

    sf = Salesforce(config_key="sales-force")
    df = sf.to_df(query="SELECT Id FROM TestObject__c")

    df1 = pd.DataFrame([{"Id": df["Id"][0], "Name": UPSERTED_ROW}])
    sf = Salesforce(config_key="sales-force")
    sf.upsert(df=df1, table=OBJECT_NAME)

    df = sf.to_df(query="SELECT FIELDS(STANDARD) FROM TestObject__c")

    assert df["Name"][0] == UPSERTED_ROW
    assert len(df.axes[1]) == 10
