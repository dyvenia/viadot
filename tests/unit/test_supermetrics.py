import pytest
import pandas as pd

from viadot.sources import Supermetrics
from viadot.config import get_source_credentials

RESPONSE_PIVOTED = {
    "meta": {
        "query": {
            "fields": [
                {
                    "id": "Date",
                    "field_id": "Date",
                    "field_name": "Date",
                    "field_type": "dim",
                    "field_split": "row",
                },
                {
                    "id": "profile",
                    "field_id": "profile",
                    "field_name": "View",
                    "field_type": "dim",
                    "field_split": "row",
                },
                {
                    "id": "segment",
                    "field_id": "segment",
                    "field_name": "Segment",
                    "field_type": "dim",
                    "field_split": "column",
                },
                {
                    "id": "Sessions",
                    "field_id": "Sessions",
                    "field_name": "Sessions",
                    "field_type": "met",
                    "field_split": "row",
                },
            ]
        },
        "result": {"total_columns": 6, "total_rows": 700},
    },
    "data": [
        [
            "Date",
            "View",
            "M-site_TOTAL: Bounces Landing",
            "M-site_TOTAL: Click to EDSP",
            "M-site_TOTAL: MQL Conversion Page Sessions",
            "M-site_TOTAL: Click to RWS",
        ],
        ["2020-01-01", "REDACTED", 123, 456, 78, 9],
    ],
}

RESPONSE_PIVOTED_NO_DATA = {
    "meta": {
        "query": {
            "fields": [
                {
                    "id": "Date",
                    "field_id": "Date",
                    "field_name": "Date",
                    "field_type": "dim",
                    "field_split": "row",
                },
                {
                    "id": "profileID",
                    "field_id": "profileID",
                    "field_name": "View ID",
                    "field_type": "dim",
                    "field_split": "row",
                },
                {
                    "id": "Hostname",
                    "field_id": "Hostname",
                    "field_name": "Hostname",
                    "field_type": "dim",
                    "field_split": "row",
                },
                {
                    "id": "profile",
                    "field_id": "profile",
                    "field_name": "View",
                    "field_type": "dim",
                    "field_split": "row",
                },
                {
                    "id": "segment",
                    "field_id": "segment",
                    "field_name": "Segment",
                    "field_type": "dim",
                    "field_split": "column",
                },
                {
                    "id": "Sessions",
                    "field_id": "Sessions",
                    "field_name": "Sessions",
                    "field_type": "met",
                    "field_split": "row",
                },
            ]
        },
        "result": {"total_columns": 0, "total_rows": 0},
    },
    "data": [],
}


def test___get_col_names_other():
    cols_list = Supermetrics._get_col_names_other(response=RESPONSE_PIVOTED)
    assert cols_list == ["Date", "View", "Segment", "Sessions"]


def test___get_col_names_google_analytics_pivoted():
    columns = Supermetrics._get_col_names_google_analytics(response=RESPONSE_PIVOTED)
    assert columns == [
        "Date",
        "View",
        "M-site_TOTAL: Bounces Landing",
        "M-site_TOTAL: Click to EDSP",
        "M-site_TOTAL: MQL Conversion Page Sessions",
        "M-site_TOTAL: Click to RWS",
    ]


def test___get_col_names_google_analytics_pivoted_no_data():
    with pytest.raises(ValueError):
        Supermetrics._get_col_names_google_analytics(response=RESPONSE_PIVOTED_NO_DATA)


def test__query() -> bool:
    config_key = "supermetrics"
    s = Supermetrics(config_key=config_key)

    google_ads_params = {
        "ds_id": "AW",
        "ds_accounts": ["1007802423"],
        "ds_user": "google@velux.com",
        "date_range_type": "last_month",
        "fields": [
            "Date",
            "Campaignname",
            "Clicks",
        ],
        "max_rows": 1,
    }

    # Expected Value
    credentials = get_source_credentials(config_key)

    assert s.query(google_ads_params).credentials == credentials


def test__to_json():
    # Create the query
    config_key = "supermetrics"
    s = Supermetrics(config_key=config_key)
    google_ads_params = {
        "ds_id": "AW",
        "ds_accounts": ["1007802423"],
        "ds_user": "google@velux.com",
        "date_range_type": "last_month",
        "fields": [
            "Date",
            "Campaignname",
            "Clicks",
        ],
        "max_rows": 1,
    }
    dict_ = s.query(google_ads_params).to_json()
    assert list(dict_.keys()) == ["meta", "data"]


def test__to_df():
    # Create the query
    config_key = "supermetrics"
    s = Supermetrics(config_key=config_key)
    google_ads_params = {
        "ds_id": "AW",
        "ds_accounts": ["1007802423"],
        "ds_user": "google@velux.com",
        "date_range_type": "last_month",
        "fields": [
            "Date",
            "Campaignname",
            "Clicks",
        ],
        "max_rows": 1,
    }
    df = s.query(google_ads_params).to_df()
    df_expected = pd.DataFrame(
        {
            "Date": "2023-03-01",
            "Campaign name": "FR : Brand VELUX (Exact)",
            "Clicks": 749,
        },
        index=[0],
    )
    assert df.equals(df_expected)


def test___get_col_names():
    # Create the query
    config_key = "supermetrics"
    s = Supermetrics(config_key=config_key)
    google_ads_params = {
        "ds_id": "AW",
        "ds_accounts": ["1007802423"],
        "ds_user": "google@velux.com",
        "date_range_type": "last_month",
        "fields": [
            "Date",
            "Campaignname",
            "Clicks",
        ],
        "max_rows": 1,
    }
    cols_list = s.query(google_ads_params)._get_col_names()
    assert cols_list == ["Date", "Campaign name", "Clicks"]
