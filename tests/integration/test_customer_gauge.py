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
