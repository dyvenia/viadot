import random

import pandas as pd
import pytest

from viadot.sources import CustomerGauge

ENDPOINT = random.choice(["responses", "non-responses"])
CG = CustomerGauge(endpoint=ENDPOINT)


def test_get_json_content():
    json_response = CG.get_json_response()
    assert isinstance(json_response, dict)
    assert isinstance(json_response["data"], list)
    assert isinstance(json_response["data"][2], dict)
    assert isinstance(json_response["cursor"], dict)


def test_properties_cleaning():
    json_response = CG.get_json_response()
    data = json_response["data"][2].copy()
    cleaned_data = CG.properties_cleaning(data.copy())
    assert isinstance(data["properties"], list)
    assert isinstance(cleaned_data["properties"], dict)


def test_flatten_json():
    nested_json = {
        "user": {
            "name": "Jane",
            "address": {
                "street": "456 Elm St",
                "city": "San Francisco",
                "state": "CA",
                "zip": "94109",
                "country": {"name": "United States", "code": "US"},
            },
            "phone_numbers": {"type": "home", "number": "555-4321"},
        }
    }

    expected_output = {
        "user_name": "Jane",
        "user_address_street": "456 Elm St",
        "user_address_city": "San Francisco",
        "user_address_state": "CA",
        "user_address_zip": "94109",
        "user_address_country_name": "United States",
        "user_address_country_code": "US",
        "user_phone_numbers_type": "home",
        "user_phone_numbers_number": "555-4321",
    }

    output = CG.flatten_json(nested_json)
    assert output == expected_output


def test_pagesize_and_to_df():
    json_response = CG.get_json_response(pagesize=1)
    df = CG.to_df(json_response)
    assert isinstance(df, pd.DataFrame)
    assert len(df) == 1


def test_pass_specific_cursor():
    # for default pagesize=1000 returned cursor value should be bigger than passed
    cur = random.randint(1, 9999)
    json_response = CG.get_json_response(cursor=cur)
    cur_retrieved = CG.get_cursor(json_response)
    assert cur_retrieved > cur


def test_uncomplete_date_arguments():
    with pytest.raises(ValueError, match="Missing date arguments"):
        json_response = CG.get_json_response(
            date_field="date_sent", start_date="2012-01-03"
        )


def test_endpoint_url_argument():
    ENDPOINT = random.choice(["responses", "non-responses"])
    ENDPOINT_URL = f"https://api.eu.customergauge.com/v7/rest/sync/{ENDPOINT}"
    CG = CustomerGauge(url=ENDPOINT_URL)
    json_response = CG.get_json_response()
    assert isinstance(json_response, dict)
