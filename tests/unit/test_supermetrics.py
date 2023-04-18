import pytest

from viadot.sources import Supermetrics

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
