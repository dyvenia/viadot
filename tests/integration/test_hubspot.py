import json

import pandas as pd
import pytest

from viadot.sources import Hubspot
from viadot.task_utils import credentials_loader

CREDENTIALS = credentials_loader.run(credentials_secret="HUBSPOT")
HUBSPOT = Hubspot(credentials=CREDENTIALS)


@pytest.fixture(scope="session")
def var_dictionary():
    variables = {
        "endpoint": "contacts",
        "filters": [
            {
                "filters": [
                    {
                        "propertyName": "createdate",
                        "operator": "BETWEEN",
                        "highValue": "1642636800000",
                        "value": "1642535000000",
                    },
                    {
                        "propertyName": "email",
                        "operator": "CONTAINS_TOKEN",
                        "value": "*@seznam.cz",
                    },
                ]
            }
        ],
        "properties": [
            "recent_conversion_date",
            "recent_conversion_event_name",
            "hs_analytics_num_visits",
        ],
    }
    yield variables


def test_clean_special_characters():
    test_value = "762##28cd7-e$69d-4708-be31-726bb!859befd"
    clean_chars = HUBSPOT.clean_special_characters(value=test_value)
    assert clean_chars == "762%2523%252328cd7-e%252469d-4708-be31-726bb%2521859befd"


def test_get_api_url(var_dictionary):
    api_url = HUBSPOT.get_api_url(
        endpoint=var_dictionary["endpoint"],
        filters=var_dictionary["filters"],
        properties=var_dictionary["properties"],
    )
    assert (
        api_url
        == "https://api.hubapi.com/crm/v3/objects/contacts/search/?limit=100&properties=recent_conversion_date,recent_conversion_event_name,hs_analytics_num_visits&"
    )


def test_get_api_body(var_dictionary):
    api_body = HUBSPOT.get_api_body(filters=var_dictionary["filters"])

    assert (
        api_body
        == '{"filterGroups": [{"filters": [{"propertyName": "createdate", "operator": "BETWEEN", "highValue": "1642636800000", "value": "1642535000000"}, {"propertyName": "email", "operator": "CONTAINS_TOKEN", "value": "*@seznam.cz"}]}], "limit": 100}'
    )


def test_to_json(var_dictionary):
    api_url = HUBSPOT.get_api_url(
        endpoint=var_dictionary["endpoint"],
        filters=var_dictionary["filters"],
        properties=var_dictionary["properties"],
    )

    api_body = HUBSPOT.get_api_body(filters=var_dictionary["filters"])

    trigger = HUBSPOT.to_json(url=api_url, body=api_body, method="POST")

    assert isinstance(trigger, dict)
