from unittest import mock
import pytest
import pandas as pd
import os
import json

from viadot.flows import HubspotToADLS

DATA = '{"id":{"0":"514733951"},"createdAt":{"0":"2023-01-01T11:04:39.774Z"},"updatedAt":{"0":"2023-03-31T07:07:46.191Z"},"archived":{"0":false},"properties.createdate":{"0":"2023-01-01T11:04:39.774Z"},"properties.email":{"0":"svob.marcela@seznam.cz"},"properties.firstname":{"0":null},"properties.hs_object_id":{"0":"514733951"},"properties.lastmodifieddate":{"0":"2023-03-31T07:07:46.191Z"},"properties.lastname":{"0":null}}'

ADLS_FILE_NAME = "test_hubspot.parquet"
ADLS_DIR_PATH = "raw/tests/"


@mock.patch(
    "viadot.tasks.HubspotToDF.run",
    return_value=pd.DataFrame(data=json.loads(DATA)),
)
@pytest.mark.run
def test_hubspot_to_adls_flow_run(mocked_class):
    flow = HubspotToADLS(
        "test_hubspot_to_adls_flow_run",
        hubspot_credentials_key="HUBSPOT",
        endpoint="contacts",
        filters=[
            {
                "filters": [
                    {
                        "propertyName": "createdate",
                        "operator": "BETWEEN",
                        "highValue": "2023-01-02",
                        "value": "2023-01-01",
                    },
                    {
                        "propertyName": "email",
                        "operator": "CONTAINS_TOKEN",
                        "value": "*marcela@seznam.cz",
                    },
                ]
            },
            {
                "filters": [
                    {
                        "propertyName": "lastmodifieddate",
                        "operator": "EQ",
                        "value": "2023-01-03",
                    }
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
