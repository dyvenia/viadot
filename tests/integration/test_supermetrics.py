from viadot.config import local_config
from viadot.sources import Supermetrics


def test_connection():
    credentials = local_config.get("SUPERMETRICS")
    s = Supermetrics()
    google_ads_params = {
        "ds_id": "AW",
        "ds_accounts": ["1007802423"],
        "ds_user": credentials["USER"],
        "date_range_type": "last_month",
        "fields": [
            "Date",
            "Campaignname",
            "Clicks",
        ],
        "max_rows": 1,
    }
    df = s.query(google_ads_params).to_df()
    assert df.count()[0] > 0
