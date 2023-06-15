import pandas as pd
import pytest

from viadot.tasks.hubspot import HubspotToDF


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


def test_hubspot_to_df(var_dictionary):
    endpoint = var_dictionary["endpoint"]
    properties = var_dictionary["properties"]
    filters = var_dictionary["filters"]
    nrows = 150

    hubspot_to_df_task = HubspotToDF(hubspot_credentials_key="HUBSPOT")

    df = hubspot_to_df_task.run(
        endpoint=endpoint, properties=properties, filters=filters, nrows=nrows
    )

    assert isinstance(df, pd.DataFrame)
