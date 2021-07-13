from ..config import local_config
from ..flows import SupermetricsToAzureSQLv3

SUPERMETRICS_CREDENTIALS = local_config.get("SUPERMETRICS")

# Note this flow uses local config, as well as Prefect and Azure Key Vault
# secrets for the Key Vault, ADLS, and Azure SQL Database.
# These defaults are set with the following local Prefect secrets:
# AZURE_CREDENTIALS, AZURE_DEFAULT_KEYVAULT,
# AZURE_DEFAULT_ADLS_SERVICE_PRINCIPAL_SECRET,
# and AZURE_DEFAULT_SQLDB_SERVICE_PRINCIPAL_SECRET.
# Make sure to specify them in your .prefect/config.toml file.
# For details on each secret, see the relevant task's documentation.

google_ads_flow = SupermetricsToAzureSQLv3(
    "Google Ads extract",
    ds_id="AW",
    ds_accounts=SUPERMETRICS_CREDENTIALS["SOURCES"]["Google Ads"]["Accounts"],
    ds_user=SUPERMETRICS_CREDENTIALS["USER"],
    date_range_type="last_year_inc",
    fields=[
        "Date",
        "profile",
        "Campaignname",
        "Impressions",
        "Clicks",
        "Cost_eur",
        "SearchImpressionShare",
    ],
    max_rows=1000000,
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
    table="google_ads_test",
    adls_path="tests/supermetrics/google_ads.csv",
)

# google_ads_flow.visualize()  # print the generated execution graph
google_ads_flow.run()
