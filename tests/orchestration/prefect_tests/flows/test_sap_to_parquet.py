from viadot.orchestration.prefect.flows import sap_to_parquet
import pandas as pd
import os

PATH ="test_path.parquet"
SAP_CREDS = "sap-dev"

def test_sap_to_parquet():
    assert os.path.isfile(PATH) is False

    flow = sap_to_parquet(
        path=PATH,
        query="""SELECT MATKL, MTART, ERSDA FROM MARA WHERE ERSDA = '20221230'""",
        func= "RFC_READ_TABLE",
        rfc_total_col_width_character_limit = 400,
        sap_credentials_secret=SAP_CREDS,
    )

    parquet_file = os.path.isfile(PATH)
    assert parquet_file is True
    
    os.remove(PATH)

