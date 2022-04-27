from viadot.flows import ADLSContainerToContainer
from viadot.sources import AzureDataLake

TEST_FILE_BLOB_PATH = "raw/supermetrics/mp/test.csv"
TEST_FILE_BLOB_PATH2 = "operations/supermetrics/mp/test.csv"


def test_adls_container_to_container():
    flow = ADLSContainerToContainer(
        name="test to container",
        from_path=TEST_FILE_BLOB_PATH,
        to_path=TEST_FILE_BLOB_PATH2,
    )
    flow.run()
    file = AzureDataLake(TEST_FILE_BLOB_PATH2)
    assert file.exists()
