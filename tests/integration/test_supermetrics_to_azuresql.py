import os

import pytest

from viadot.config import local_config
from viadot.sources import AzureBlobStorage, AzureSQL, Supermetrics

SUPERMETRICS_CREDENTIALS = local_config.get("SUPERMETRICS")
FILE_PATH = "/home/viadot/tests/integration/test_supermetrics.csv"
BLOB_PATH = "testing/supermetrics/test_supermetrics.csv"

SCHEMA = "sandbox"
TABLE = "test_supermetrics"


@pytest.fixture
def supermetrics():
    supermetrics = Supermetrics(config_key="SUPERMETRICS")
    yield supermetrics


@pytest.fixture
def azstorage():
    azstorage = AzureBlobStorage(config_key="AZURE_BLOB_STORAGE")
    yield azstorage
    os.remove(FILE_PATH)


# Replace these hacks later
# Date
dtypes = {
    "Date": "DATE",
    "profile": "VARCHAR(255)",
    "Campaignname": "VARCHAR(255)",
    "Impressions": "FLOAT(24)",
    "Clicks": "FLOAT(24)",
    "Cost_eur": "FLOAT(24)",
    "SearchImpressionShare": "VARCHAR(255)",
}
# End crappy hacks

sm_query = {
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
}


def test_fetch_supermetrics(supermetrics):
    supermetrics.query(sm_query)
    supermetrics.to_csv(FILE_PATH)


def test_upload(azstorage):
    azstorage.to_storage(from_path=FILE_PATH, to_path=BLOB_PATH, overwrite=True)


def test_bulk_insert():
    azuresql = AzureSQL(config_key="AZURE_SQL")
    azuresql.create_table(
        schema=SCHEMA, table=TABLE, dtypes=dtypes, if_exists="replace"
    )
    azuresql.bulk_insert(
        table=TABLE,
        schema=SCHEMA,
        source_path=BLOB_PATH,
        if_exists="replace",
    )
