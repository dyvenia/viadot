from viadot.orchestration.prefect.flows import sap_to_parquet
from viadot.orchestration.prefect.utils import get_credentials
import pandas as pd
import os

PATH ="data/billing_type_full_data.parquet"
SAP_CREDS = get_credentials("sap-credentials")

def test_sap_to_parquet():
    flow = sap_to_parquet(
        path=PATH,
        query=""" select 
            FKART as billing_type
            ,VTEXT as billing_type_name
            ,SPRAS as language_key    
            from TVFKT  
            where SPRAS in ('E', 'D')""",
        func= "RFC_READ_TABLE",
        rfc_total_col_width_character_limit = 400,
        sap_credentials_secret="SAP_CREDS",
    )

    parquet_file = os.path.exists(PATH)
    assert parquet_file is True
    
    os.remove(PATH)