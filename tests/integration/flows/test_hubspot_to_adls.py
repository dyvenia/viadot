import json
import os
from unittest import mock

import pandas as pd
import pytest

from viadot.flows import HubspotToADLS

DATA = {
    "id": {"0": "820306930"},
    "createdAt": {"0": "2020-08-31T11:58:55.762Z"},
    "updatedAt": {"0": "2021-07-17T05:25:41.480Z"},
    "archived": {"0": "False"},
    "properties.amount": {"0": "3744.000"},
    "properties.createdate": {"0": "2020-08-31T11:58:55.762Z"},
    "properties.hs_lastmodifieddate": {"0": "2021-07-17T05:25:41.480Z"},
    "properties.hs_object_id": {"0": "820306930"},
    "properties.hs_product_id": {"0": "2344"},
    "properties.quantity": {"0": "2"},
}

ADLS_FILE_NAME = "test_hubspot.parquet"
ADLS_DIR_PATH = "raw/tests/"


@mock.patch(
    "viadot.tasks.HubspotToDF.run",
    return_value=pd.DataFrame(data=DATA),
)
@pytest.mark.run
def test_hubspot_to_adls_flow_run(mocked_class):
    flow = HubspotToADLS(
        "test_hubspot_to_adls_flow_run",
        hubspot_credentials_key="HUBSPOT",
        endpoint="line_items",
        filters=[
            {
                "filters": [
                    {
                        "propertyName": "createdate",
                        "operator": "BETWEEN",
                        "highValue": "2021-01-01",
                        "value": "2021-01-01",
                    },
                    {"propertyName": "quantity", "operator": "EQ", "value": "2"},
                ]
            },
            {
                "filters": [
                    {"propertyName": "amount", "operator": "EQ", "value": "3744.000"}
                ]
            },
        ],
        overwrite_adls=True,
        adls_dir_path=ADLS_DIR_PATH,
        adls_file_name=ADLS_FILE_NAME,
    )
    result = flow.run()
    assert result.is_successful()
    os.remove("test_hubspot_to_adls_flow_run.parquet")
    os.remove("test_hubspot_to_adls_flow_run.json")
