from viadot.config import local_config
from viadot.tasks import SAPRFCToDF


def test_sap_rfc_to_df_bbp():
    sap_test_creds = local_config.get("SAP").get("QA")
    task = SAPRFCToDF(
        credentials=sap_test_creds,
        query="SELECT MATNR, MATKL, MTART, LAEDA FROM MARA WHERE LAEDA LIKE '2022%'",
        func="BBP_RFC_READ_TABLE",
    )
    df = task.run()
    assert len(df.columns) == 4 and not df.empty
