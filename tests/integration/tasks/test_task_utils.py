import logging
import pytest
import pandas as pd

from viadot.tasks import AzureSQLCreateTable
from viadot.exceptions import ValidationError
from viadot.task_utils import EnsureDFColumnOrder


SCHEMA = "sandbox"
TABLE = "test"


def test_ensure_df_column_order_append_same_col_number(caplog):
    create_table_task = AzureSQLCreateTable()
    with caplog.at_level(logging.INFO):
        create_table_task.run(
            schema=SCHEMA,
            table=TABLE,
            dtypes={"id": "INT", "name": "VARCHAR(25)", "street": "VARCHAR(25)"},
            if_exists="replace",
        )
    assert "Successfully created table sandbox" in caplog.text

    data = {"id": [1], "street": ["Green"], "name": ["Tom"]}
    df = pd.DataFrame(data)

    ensure_df_column_order = EnsureDFColumnOrder()
    with caplog.at_level(logging.WARNING):
        ensure_df_column_order.run(
            table=TABLE,
            schema=SCHEMA,
            if_exists="append",
            df=df,
            config_key="AZURE_SQL",
        )

        assert (
            "Detected column order difference between the CSV file and the table. Reordering..."
            in caplog.text
        )


def test_ensure_df_column_order_append_diff_col_number(caplog):
    create_table_task = AzureSQLCreateTable()
    with caplog.at_level(logging.INFO):
        create_table_task.run(
            schema=SCHEMA,
            table=TABLE,
            dtypes={"id": "INT", "name": "VARCHAR(25)", "street": "VARCHAR(25)"},
            if_exists="replace",
        )
    assert "Successfully created table sandbox" in caplog.text

    data = {"id": [1], "age": ["40"], "street": ["Green"], "name": ["Tom"]}
    df = pd.DataFrame(data)
    print(f"COMP: \ndf: {df.columns} \nsql: ")
    ensure_df_column_order = EnsureDFColumnOrder()
    with pytest.raises(
        ValidationError,
        match=r"Detected discrepancies in number of columns or different column names between the CSV file and the SQL table!",
    ):
        ensure_df_column_order.run(
            table=TABLE,
            schema=SCHEMA,
            if_exists="append",
            df=df,
            config_key="AZURE_SQL",
        )


def test_ensure_df_column_order_replace(caplog):
    create_table_task = AzureSQLCreateTable()
    with caplog.at_level(logging.INFO):
        create_table_task.run(
            schema=SCHEMA,
            table=TABLE,
            dtypes={"id": "INT", "name": "VARCHAR(25)", "street": "VARCHAR(25)"},
            if_exists="replace",
        )
    assert "Successfully created table sandbox" in caplog.text

    data = {"id": [1], "street": ["Green"], "name": ["Tom"]}
    df = pd.DataFrame(data)

    ensure_df_column_order = EnsureDFColumnOrder()
    with caplog.at_level(logging.INFO):
        ensure_df_column_order.run(
            table=TABLE, if_exists="replace", df=df, config_key="AZURE_SQL"
        )
    assert "The table will be replaced." in caplog.text


def test_ensure_df_column_order_append_not_exists(caplog):
    ensure_df_column_order = EnsureDFColumnOrder()
    data = {"id": [1], "street": ["Green"], "name": ["Tom"]}
    df = pd.DataFrame(data)
    ensure_df_column_order.run(
        table="non_existing_table_123",
        schema="sandbox",
        if_exists="append",
        df=df,
        config_key="AZURE_SQL",
    )
    assert "table doesn't exists" in caplog.text
