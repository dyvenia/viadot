import pytest

from viadot.config import get_source_config
from viadot.orchestration.prefect.flows import supermetrics_to_adls


@pytest.mark.parametrize(
    ("supermetrics_config_key", "adls_credentials_secret"),
    [
        ("supermetrics", "supermetrics"),
    ],
)
def test_supermetrics_to_adls(supermetrics_config_key, adls_credentials_secret):
    supermetrics_config = get_source_config(supermetrics_config_key)
    google_ads_params = {
        "ds_id": "AW",
        "ds_user": supermetrics_config["credentials"].get("user"),
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
        query_params=google_ads_params,
        supermetrics_config_key=supermetrics_config_key,
        adls_credentials_secret=adls_credentials_secret,
        overwrite=True,
        adls_path="raw/supermetrics/.parquet",
    )

    all_successful = all(s.type == "COMPLETED" for s in state)
    assert all_successful, "Not all tasks in the flow completed successfully."
