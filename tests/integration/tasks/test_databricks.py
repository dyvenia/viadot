import pyspark
from viadot.sources import Databricks
import pandas as pd
import pytest

databricks = Databricks(env="QA")
schema = "afraijat_test"

source_data = [
    {
        "Id": "wRACnHTeuw",
        "AccountId": 123,
        "Name": "Scott-Merritt",
        "FirstName": "Melody",
        "LastName": "Cook",
        "ContactEMail": "Melody.Cook@ScottMerritt.com",
        "MailingCity": "Elizabethfurt",
    },
    {
        "Id": "CFfTlqagNlpDu",
        "AccountId": 456,
        "Name": "Mann-Warren",
        "FirstName": "Wayne",
        "LastName": "Morrison",
        "ContactEMail": "Wayne.Morrison@MannWarren.com",
        "MailingCity": "Kathrynmouth",
    },
]
df = pd.DataFrame(source_data)


def test_to_df():
    # Look up test_query table and check if the type and results returned are correct
    table = "test_query"
    fqn = f"{schema}.{table}"

    result = databricks.to_df(f"select * from {fqn}")

    assert result.shape == df.shape


def test_create_table():

    table = "test_create_table"

    fqn = f"{schema}.{table}"

    exists = databricks._check_if_table_exists(schema, table)

    assert not exists

    databricks.create_table_from_pandas(schema, table, df)

    result = databricks.to_df(f"SELECT * FROM {fqn}")

    assert result.shape == df.shape
    databricks.drop_table(schema, table)


def test_append():
    append_data = [
        {
            "Id": "UpsertTest2",
            "AccountId": 789,
            "Name": "new upsert-2",
            "FirstName": "Updated",
            "LastName": "Carter2",
            "ContactEMail": "Adam.Carter@TurnerBlack.com",
            "MailingCity": "Updated!Jamesport",
        }
    ]

    table = "test_append_table"

    fqn = f"{schema}.{table}"

    databricks.create_table_from_pandas(schema, table, df)

    append_df = pd.DataFrame(append_data)

    did_insert = databricks.insert_into(
        schema, table=table, df=append_df, if_exists="append"
    )
    assert did_insert

    expected_result = df.append(append_df)

    result = databricks.to_df(f"SELECT * FROM {fqn}")

    assert result.shape == expected_result.shape

    databricks.drop_table(schema, table)


def test_insert_wrong_schema():
    append_data = [
        {
            "Id": "UpsertTest2",
            "AccountId": 789,
            "Name": "new upsert-2",
            "FirstName": "Updated",
            "LastName": "Carter2",
            "ContactEMail": "Adam.Carter@TurnerBlack.com",
            "MailingCity": "Updated!Jamesport",
            "WrongField": "Wrong field",
        }
    ]

    table = "test_table"

    append_df = pd.DataFrame(append_data)

    with pytest.raises(pyspark.sql.utils.AnalysisException):
        did_insert = databricks.insert_into(
            schema, table=table, df=append_df, if_exists="append"
        )


def test_insert_non_existent_table():
    append_data = [
        {
            "Id": "UpsertTest2",
            "AccountId": 789,
            "Name": "new upsert-2",
            "FirstName": "Updated",
            "LastName": "Carter2",
            "ContactEMail": "Adam.Carter@TurnerBlack.com",
            "MailingCity": "Updated!Jamesport",
            "WrongField": "Wrong field",
        }
    ]

    table = "test_non_existent_table"

    append_df = pd.DataFrame(append_data)

    with pytest.raises(ValueError):
        did_insert = databricks.insert_into(
            schema, table=table, df=append_df, if_exists="append"
        )


def test_full_refresh():
    # Assert type and values returned after full refresh

    table = "test_full_refresh_table"
    fqn = f"{schema}.{table}"

    databricks.create_table_from_pandas(schema, table, df)

    full_refresh_data = [
        {
            "Id": "wRACnHTeuw",
            "AccountId": 123,
            "Name": "Scott-Merritt",
            "FirstName": "Melody",
            "LastName": "Cook",
            "ContactEMail": "Melody.Cook@ScottMerritt.com",
            "MailingCity": "Elizabethfurt",
        }
    ]

    full_refresh_df = pd.DataFrame(full_refresh_data)

    did_insert = databricks.insert_into(
        schema=schema, table=table, df=full_refresh_df, if_exists="replace"
    )

    assert did_insert

    result = databricks.to_df(f"SELECT * FROM {fqn}")

    assert result.shape == full_refresh_df.shape
    databricks.drop_table(schema, table)


def test_upsert():
    # Upsert and check if the data type and values are correct

    upsert_data = [
        {
            "Id": "UpsertTest2",
            "AccountId": "Updated!EHNYKjSZsiy",
            "Name": "new upsert-2",
            "FirstName": "Updated",
            "LastName": "Carter2",
            "ContactEMail": "Adam.Carter@TurnerBlack.com",
            "MailingCity": "Updated!Jamesport",
        }
    ]

    table = "test_upsert"
    fqn = f"{schema}.{table}"
    primary_key = "Id"

    databricks.create_table_from_pandas(schema, table, df)

    upsert_df = pd.DataFrame(upsert_data)

    did_insert = databricks.insert_into(
        schema=schema,
        table=table,
        df=upsert_df,
        primary_key=primary_key,
        if_exists="update",
    )

    assert did_insert

    expected_result = df.append(upsert_df)

    result = databricks.to_df(f"SELECT * FROM {fqn}")

    assert result.shape == expected_result.shape

    databricks.drop_table(schema, table)


def test_discover_schema():
    table = "test_table"
    schema_result = databricks.discover_schema(schema, table)
    expected_schema = {
        "Id": "string",
        "AccountId": "bigint",
        "Name": "string",
        "FirstName": "string",
        "LastName": "string",
        "ContactEMail": "string",
        "MailingCity": "string",
    }

    assert schema_result == expected_schema
