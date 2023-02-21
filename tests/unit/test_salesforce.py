import pandas as pd
import pytest
from viadot.sources import Salesforce

OBJECT_NAME = "TestObject__c"
ROW_NAME_1 = "Test_row_1"
ROW_NAME_2 = "Test_row_2"
UPSERTED_ROW_1 = "Test_upser_1"
UPSERTED_ROW_2 = "Test_upser_2"


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
    sf_api.TestObject__c.create({"Name": ROW_NAME_1})
    sf_api.TestObject__c.create({"Name": ROW_NAME_2})

    yield

    mdapi.CustomObject.delete(OBJECT_NAME)


def test_to_df_selected_table(setting_up_test_environment):

    sf = Salesforce(config_key="sales-force")
    df = sf.to_df(table=OBJECT_NAME)
    print(df)
    assert df["Name"][0] == ROW_NAME_1
    assert df["Name"][1] == ROW_NAME_2
    assert len(df.axes[1]) == 10


def test_to_df_selected_table_query(setting_up_test_environment):
    sf = Salesforce(config_key="sales-force")
    df = sf.to_df(query=f"SELECT FIELDS(STANDARD) FROM {OBJECT_NAME}")
    assert df["Name"][0] == ROW_NAME_1
    assert df["Name"][1] == ROW_NAME_2
    assert len(df.axes[1]) == 10


def test_upsert_existing_row(setting_up_test_environment):

    sf = Salesforce(config_key="sales-force")
    df = sf.to_df(query=f"SELECT Id FROM {OBJECT_NAME}")

    df1 = pd.DataFrame([{"Id": df["Id"][0], "Name": UPSERTED_ROW_1}])
    sf = Salesforce(config_key="sales-force")
    sf.upsert(df=df1, table=OBJECT_NAME)

    df = sf.to_df(query=f"SELECT FIELDS(STANDARD) FROM {OBJECT_NAME}")

    assert df["Name"][0] == UPSERTED_ROW_1
    assert len(df.axes[1]) == 10


def test_bulk_upsert_existing_rows():

    sf = Salesforce(config_key="sales-force")
    df = sf.to_df(query=f"SELECT Id FROM {OBJECT_NAME}")

    df1 = pd.DataFrame(
        [
            {"Id": df["Id"][0], "Name": UPSERTED_ROW_1},
            {"Id": df["Id"][1], "Name": UPSERTED_ROW_2},
        ]
    )
    sf = Salesforce(config_key="sales-force")
    sf.upsert(df=df1, table=OBJECT_NAME)

    df = sf.to_df(query=f"SELECT FIELDS(STANDARD) FROM {OBJECT_NAME}")
    print(df)
    assert df["Name"][1] == UPSERTED_ROW_2
    assert len(df.axes[1]) == 10


"""
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
mdapi.CustomObject.delete(OBJECT_NAME)
# mdapi.CustomObject.create(custom_object)
# sf_api.TestObject__c.create({"Name": ROW_NAME_1})
# sf_api.TestObject__c.create({"Name": ROW_NAME_2})

# df = sf.to_df(table=OBJECT_NAME)
# print(df)
"""
