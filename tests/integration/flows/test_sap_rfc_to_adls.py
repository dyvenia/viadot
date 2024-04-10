from viadot.config import local_config
from viadot.exceptions import ValidationError
from viadot.flows import SAPRFCToADLS
from viadot.sources import AzureDataLake
from viadot.tasks import AzureDataLakeRemove

try:
    import pyrfc
except ModuleNotFoundError:
    raise

ADLS_PATH = "raw/supermetrics/mp/test_file_sap.parquet"
FILE_NAME = "test_file.parquet"
SAP_TEST_CREDS = local_config.get("SAP").get("QA")


def test_sap_rfc_to_adls_query():
    flow = SAPRFCToADLS(
        name="test flow",
        query="SELECT MATNR, MATKL FROM MARA WHERE LAEDA LIKE '2022%' LIMIT 5",
        func="BBP_RFC_READ_TABLE",
        credentials=SAP_TEST_CREDS,
        local_file_path=FILE_NAME,
        adls_path=ADLS_PATH,
        overwrite=True,
    )
    result = flow.run()
    assert result.is_successful()
    file = AzureDataLake(ADLS_PATH)
    assert file.exists()
    assert len(flow.tasks) == 3
    AzureDataLakeRemove(ADLS_PATH)


def test_sap_rfc_to_adls_validation_fail():
    flow = SAPRFCToADLS(
        name="test flow",
        query="SELECT MATNR, MATKL FROM MARA WHERE LAEDA LIKE '2022%' LIMIT 5",
        func="BBP_RFC_READ_TABLE",
        credentials=SAP_TEST_CREDS,
        local_file_path=FILE_NAME,
        adls_path=ADLS_PATH,
        overwrite=True,
        validate_df_dict={"column_list_to_match": ["MATNR"]},
    )
    try:
        result = flow.run()
    except ValidationError:
        pass


def test_sap_rfc_to_adls_validation_success():
    flow = SAPRFCToADLS(
        name="test flow",
        query="SELECT MATNR, MATKL FROM MARA WHERE LAEDA LIKE '2022%' LIMIT 5",
        func="BBP_RFC_READ_TABLE",
        credentials=SAP_TEST_CREDS,
        local_file_path=FILE_NAME,
        adls_path=ADLS_PATH,
        overwrite=True,
        validate_df_dict={"column_list_to_match": ["MATNR", "MATKL"]},
    )
    result = flow.run()
    assert result.is_successful()
    file = AzureDataLake(ADLS_PATH)
    assert file.exists()
    assert len(flow.tasks) == 4
    AzureDataLakeRemove(ADLS_PATH)
