from viadot.config import local_config
from viadot.flows import SAPRFCToADLS
from viadot.sources import AzureDataLake
from viadot.tasks import AzureDataLakeRemove

try:
    import pyrfc
except ModuleNotFoundError:
    raise

ADLS_PATH = "raw/supermetrics/mp/test_file_sap.parquet"
FILE_NAME = "test_file.parquet"


def test_sap_rfc_to_adls_query():
    sap_test_creds = local_config.get("SAP").get("QA")
    flow = SAPRFCToADLS(
        name="test flow",
        query="SELECT MATNR, MATKL FROM MARA WHERE LAEDA LIKE '2022%'",
        func="BBP_RFC_READ_TABLE",
        sap_credentials=sap_test_creds,
        local_file_path=FILE_NAME,
        adls_path=ADLS_PATH,
        overwrite=True,
    )
    result = flow.run()
    assert result.is_successful()
    file = AzureDataLake(ADLS_PATH)
    assert file.exists()
    AzureDataLakeRemove(ADLS_PATH)
