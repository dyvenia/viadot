import logging
import pytest
import pandas as pd

from prefect.tasks.secrets import PrefectSecret

from viadot.tasks import AzureSQLCreateTable
from viadot.exceptions import ValidationError
from viadot.task_utils import EnsureDFColumnOrder


SCHEMA = "sandbox"
TABLE = "test"

CREDENTIALS = PrefectSecret("AZURE_DEFAULT_SQLDB_SERVICE_PRINCIPAL_SECRET").run()


@pytest.fixture(scope="session")
def create_table():
    create_table_task = AzureSQLCreateTable()
    create_table_task.run(
        schema=SCHEMA,
        table=TABLE,
        dtypes={"id": "INT", "name": "VARCHAR(25)", "street": "VARCHAR(25)"},
        if_exists="replace",
    )


@pytest.fixture(scope="session")
def ensure_df_column_order():
    task = EnsureDFColumnOrder()
    yield task


def test_ensure_df_column_order_append_same_col_number(
    create_table, ensure_df_column_order, caplog
):
    create_table
    data = {"id": [1], "street": ["Green"], "name": ["Tom"]}
    df = pd.DataFrame(data)
    with caplog.at_level(logging.WARNING):
        ensure_df_column_order.run(
            table=TABLE,
            schema=SCHEMA,
            if_exists="append",
            df=df,
            credentials_secret=CREDENTIALS,
        )

        assert (
            "Detected column order difference between the CSV file and the table. Reordering..."
            in caplog.text
        )


def test_ensure_df_column_order_append_diff_col_number(
    create_table, ensure_df_column_order
):
    create_table
    data = {"id": [1], "age": ["40"], "street": ["Green"], "name": ["Tom"]}
    df = pd.DataFrame(data)
    print(f"COMP: \ndf: {df.columns} \nsql: ")
    with pytest.raises(
        ValidationError,
        match=r"Detected discrepancies in number of columns or different column names between the CSV file and the SQL table!",
    ):
        ensure_df_column_order.run(
            table=TABLE,
            schema=SCHEMA,
            if_exists="append",
            df=df,
            credentials_secret=CREDENTIALS,
        )


def test_ensure_df_column_order_replace(create_table, ensure_df_column_order, caplog):
    create_table
    data = {"id": [1], "street": ["Green"], "name": ["Tom"]}
    df = pd.DataFrame(data)
    with caplog.at_level(logging.INFO):
        ensure_df_column_order.run(
            table=TABLE, if_exists="replace", df=df, credentials_secret=CREDENTIALS
        )
    assert "The table will be replaced." in caplog.text


def test_ensure_df_column_order_append_not_exists(ensure_df_column_order, caplog):
    data = {"id": [1], "street": ["Green"], "name": ["Tom"]}
    df = pd.DataFrame(data)
    ensure_df_column_order.run(
        table="non_existing_table_123",
        schema=SCHEMA,
        if_exists="append",
        df=df,
        credentials_secret=CREDENTIALS,
    )
    assert "table doesn't exists" in caplog.text


def test_ensure_df_column_order_replace_with_defined_dtypes(
    create_table, ensure_df_column_order
):
    create_table
    data = {"id": [1], "street": ["Green"], "name": ["Tom"]}
    df = pd.DataFrame(data)
    dtypes = {"id": "INT", "name": "VARCHAR(25)", "street": "VARCHAR(25)"}
    expected_columns_list = list(dtypes.keys())
    final_df = ensure_df_column_order.run(
        table=TABLE,
        if_exists="replace",
        df=df,
        dtypes=dtypes,
        credentials_secret=CREDENTIALS,
    )
    final_df_columns = list(final_df.columns)
    assert final_df_columns == expected_columns_list
