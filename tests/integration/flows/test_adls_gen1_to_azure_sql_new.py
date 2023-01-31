import os
from unittest import mock

import pandas as pd
import pytest

from viadot.flows import ADLSGen1ToAzureSQLNew

d = {"country": [1, 2], "sales": [3, 4]}
df = pd.DataFrame(data=d)
SCHEMA = "sandbox"
TABLE = "test_bcp"


def test_adls_gen1_to_azure_sql_new_init_args():
    flow = ADLSGen1ToAzureSQLNew(
        name="test_adls_gen1_gen2_flow",
        gen1_path="test_file_1.csv",
        gen2_path="test_file_2.csv",
        schema=SCHEMA,
        table=TABLE,
        dtypes={"country": "INT", "sales": "INT"},
        if_exists="replace",
    )

    assert flow


def test_adls_gen1_to_azure_sql_new_mock():
    with mock.patch.object(
        ADLSGen1ToAzureSQLNew, "run", return_value=True
    ) as mock_method:
        instance = ADLSGen1ToAzureSQLNew(
            name="test_adls_gen1_gen2_flow",
            gen1_path="folder1/example_file.csv",
            gen2_path="folder2/example_file.csv",
            schema="sandbox",
            table="test_bcp",
            dtypes={"country": "VARCHAR(25)", "sales": "INT"},
            if_exists="replace",
        )
        instance.run()
        mock_method.assert_called_with()


@mock.patch(
    "viadot.tasks.AzureDataLakeToDF.run",
    return_value=pd.DataFrame(data={"country": [1, 2], "sales": [3, 4]}),
)
@pytest.mark.run
def test_adls_gen1_to_azure_sql_new_flow_run_mock(mocked_class):
    flow = ADLSGen1ToAzureSQLNew(
        name="test_adls_g1g2",
        gen1_path="example_path",
        gen2_path="raw/test/test.csv",
        dtypes={"country": "VARCHAR(25)", "sales": "INT"},
        if_exists="replace",
        table="test",
        schema="sandbox",
    )

    result = flow.run()

    assert result.is_successful()
    os.remove("test_adls_g1g2.csv")
