from prefect.storage import github

from ..config import local_config
from ..flows.supermetrics_to_azure_sql import SupermetricsToAzureSQL

SUPERMETRICS_CREDENTIALS = local_config.get("SUPERMETRICS")

google_ads_flow = SupermetricsToAzureSQL(
    "Google Ads extract",
    query={
        "ds_id": "AW",
        "ds_accounts": SUPERMETRICS_CREDENTIALS["SOURCES"]["Google Ads"]["Accounts"],
        "ds_user": SUPERMETRICS_CREDENTIALS["USER"],
        "date_range_type": "last_year_inc",
        "fields": [
            "Date",
            "profile",
            "Campaignname",
            "Impressions",
            "Clicks",
            "Cost_eur",
            "SearchImpressionShare",
        ],
        "max_rows": 1000000,
    },
    dtypes={
        "Date": "DATE",
        "Account": "VARCHAR(255)",
        "Campaign": "VARCHAR(255)",
        "Impressions": "FLOAT(24)",
        "Clicks": "FLOAT(24)",
        "Cost": "FLOAT(24)",
        "ImpressionShare": "VARCHAR(255)",
    },
    schema="sandbox",
    table="google_ads",
    blob_path="tests/supermetrics/google_ads.csv",
)

# google_ads_flow.visualize()
google_ads_flow.run()
