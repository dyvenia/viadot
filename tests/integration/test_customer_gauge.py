import random

import pandas as pd
import pytest

from viadot.exceptions import CredentialError
from viadot.sources import CustomerGauge

ENDPOINT = random.choice(["responses", "non-responses"])
CG = CustomerGauge(endpoint=ENDPOINT)


def test_wrong_endpoint():
    with pytest.raises(ValueError, match="Incorrect endpoint name"):
        CustomerGauge(endpoint=["wrong_endpoint"])


def test_endpoint_and_url_not_provided():
    with pytest.raises(ValueError, match="Provide endpoint name"):
        CustomerGauge()


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


def test_cursor_is_not_provided():
    with pytest.raises(
        ValueError, match="Provided argument doesn't contain 'cursor' value"
    ):
        CG.get_cursor(json_response={})


def test_uncomplete_date_arguments():
    with pytest.raises(ValueError, match="Missing date arguments"):
        CG.get_json_response(date_field="date_sent", start_date="2012-01-03")


def test_endpoint_url_argument():
    ENDPOINT = random.choice(["responses", "non-responses"])
    ENDPOINT_URL = f"https://api.eu.customergauge.com/v7/rest/sync/{ENDPOINT}"
    CG = CustomerGauge(url=ENDPOINT_URL)
    json_response = CG.get_json_response()
    assert isinstance(json_response, dict)


@pytest.mark.endpoint_valueerror
def test_wrong_endpoint_valueerror_raising():
    with pytest.raises(
        ValueError,
        match=r"Incorrect endpoint name. Choose: 'responses' or 'non-responses'",
    ):
        wrong_endpoint_name = "wrong-endpoint"
        CG = CustomerGauge(endpoint=wrong_endpoint_name)


@pytest.mark.endpoint_valueerror
def test_no_endpoint_valueerror_raising():
    with pytest.raises(
        ValueError,
        match=r"Provide endpoint name. Choose: 'responses' or 'non-responses'. Otherwise, provide URL",
    ):
        CG = CustomerGauge()


@pytest.mark.endpoint_credentialserror
def test_credentialserror_raising():
    wrong_secret = "wrong"
    with pytest.raises(CredentialError, match=r"Credentials not provided."):
        CG = CustomerGauge(endpoint=ENDPOINT, credentials_secret=wrong_secret)


@pytest.mark.get_cursor_valueerror
def test_get_cursor_valueerror_raising():
    wrong_json = {}
    with pytest.raises(
        ValueError,
        match=r"Provided argument doesn't contain 'cursor' value. Pass json returned from the endpoint.",
    ):
        CG = CustomerGauge(endpoint=ENDPOINT)
        CG.get_cursor(json_response=wrong_json)
