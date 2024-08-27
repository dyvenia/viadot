from viadot.config import get_source_config
from viadot.orchestration.prefect.flows import supermetrics_to_adls
import pdb

def test_flow_call():
    google_ads_params = {
        "ds_id": "AW",
        "ds_user": "google@velux.com",
        "ds_accounts": ["1007802423"],
        "date_range_type": "last_month",
        "fields": [
            "Date",
            "Campaignname",
            "Clicks",
        ],
        "max_rows": 1,
    }

    state = supermetrics_to_adls(
        query_params = google_ads_params,
        supermetrics_credentials_secret = "supermetrics",
        adls_credentials_secret = "app-azure-cr-datalakegen2-dev",
        overwrite=True,   
        adls_path="raw/supermetrics/.parquet",
    )
    
    all_successful = all(s.type == "COMPLETED" for s in state)
    assert all_successful, "Not all tasks in the flow completed successfully."