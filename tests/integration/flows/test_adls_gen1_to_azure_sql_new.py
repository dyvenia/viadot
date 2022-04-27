from viadot.flows import ADLSGen1ToAzureSQLNew
import pandas as pd
import pytest
from unittest.mock import MagicMock
from viadot.flows.adls_to_azure_sql import df_to_csv_task
from viadot.task_utils import METADATA_COLUMNS, add_ingestion_metadata_task
from viadot.flows.adls_gen1_to_azure_sql_new import df_replace_special_chars
from viadot.tasks import AzureDataLakeUpload, AzureSQLCreateTable, BCPTask
from unittest import mock

gen2_upload_task = AzureDataLakeUpload(gen=2)
create_table_task = AzureSQLCreateTable()
bulk_insert_task = BCPTask()

d = {"col1": [1, 2], "col2": [3, 4]}
df = pd.DataFrame(data=d)
SCHEMA = "sandbox"
TABLE = "test_bcp"


@pytest.fixture()
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


@pytest.fixture()
def test_adls_gen1_to_azure_sql_new_run():
    class TestMocker(ADLSGen1ToAzureSQLNew):
        def gen_flow(self):
            d = {"country": [1, 2], "sales": [3, 4]}
            df = pd.DataFrame(data=d)

            df2 = df_replace_special_chars.bind(df=df, flow=self)
            df_with_metadata = add_ingestion_metadata_task.bind(df=df2, flow=self)
            df_to_csv_task.bind(
                df=df_with_metadata,
                path=self.local_file_path,
                sep=self.write_sep,
                flow=self,
                remove_tab=True,
            )
            gen2_upload_task.bind(
                from_path=self.local_file_path,
                to_path=self.gen2_path,
                overwrite=self.overwrite,
                sp_credentials_secret=self.gen2_sp_credentials_secret,
                vault_name=self.vault_name,
                flow=self,
            )
            create_table_task.bind(
                schema=self.schema,
                table=self.table,
                dtypes=self.dtypes,
                if_exists=self.if_exists,
                credentials_secret=self.sqldb_credentials_secret,
                vault_name=self.vault_name,
                flow=self,
            )
            bulk_insert_task.bind(
                path=self.local_file_path,
                schema=self.schema,
                table=self.table,
                credentials_secret=self.sqldb_credentials_secret,
                vault_name=self.vault_name,
                flow=self,
            )

            df_with_metadata.set_upstream(df_replace_special_chars, flow=self)
            df_to_csv_task.set_upstream(df_with_metadata, flow=self)
            gen2_upload_task.set_upstream(df_to_csv_task, flow=self)
            create_table_task.set_upstream(df_to_csv_task, flow=self)
            bulk_insert_task.set_upstream(create_table_task, flow=self)

    flow = TestMocker(
        name="test_adls_gen1_gen2_flow",
        gen1_path="test_file_1.csv",
        gen2_path="raw/supermetrics/test_file_2.csv",
        schema=SCHEMA,
        table=TABLE,
        dtypes={"country": "VARCHAR(25)", "sales": "INT"},
        if_exists="replace",
    )
    assert flow.run()


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
