import contextlib

import pyarrow as pa
import pytest

from viadot.sources import Trino


TEST_BUCKET = "spark"
TEST_SCHEMA = "test_schema"
TEST_SCHEMA_PATH = f"s3a://{TEST_BUCKET}/{TEST_SCHEMA}"
TEST_TABLE = "test_table"
TEST_TABLE_PATH = f"{TEST_SCHEMA_PATH}/{TEST_TABLE}"


@pytest.fixture(scope="session")
def trino(trino_config_key):
    trino = Trino(config_key=trino_config_key)

    with contextlib.suppress(Exception):
        trino.drop_schema(TEST_SCHEMA, cascade=True)
    return trino


def test_get_schemas(trino):
    schemas = trino.get_schemas()
    assert "default" in schemas


def test_create_iceberg_schema(trino):
    # Assumptions.
    exists = trino._check_if_schema_exists(TEST_SCHEMA)
    assert exists is False

    trino.create_iceberg_schema(TEST_SCHEMA, location=TEST_SCHEMA_PATH)

    exists = trino._check_if_schema_exists(TEST_SCHEMA)
    assert exists is True

    # Cleanup.
    trino.drop_schema(TEST_SCHEMA, cascade=True)


def test_drop_schema(trino):
    # Assumptions.
    trino.create_iceberg_schema(TEST_SCHEMA, location=TEST_SCHEMA_PATH)
    exists = trino._check_if_schema_exists(TEST_SCHEMA)
    assert exists is True

    trino.drop_schema(TEST_SCHEMA, cascade=True)
    exists = trino._check_if_schema_exists(TEST_SCHEMA)
    assert exists is False


def test__create_table_query_basic(trino, DF):
    """Test that the most basic create table query is construed as expected."""
    pa_table = pa.Table.from_pandas(DF)
    test_df_columns = pa_table.schema.names
    test_df_types = [
        trino.pyarrow_to_trino_type(str(typ)) for typ in pa_table.schema.types
    ]

    query = trino._create_table_query(
        schema_name=TEST_SCHEMA,
        table_name=TEST_TABLE,
        columns=test_df_columns,
        types=test_df_types,
    )

    expected_query = f"""
CREATE TABLE {TEST_SCHEMA}.{TEST_TABLE} (
    country VARCHAR,\n\tsales BIGINT
)
WITH (
    format = 'PARQUET'
)"""
    assert query == expected_query, query


def test__create_table_query_partitions(trino, DF):
    """Test create table query is construed as expected when partitions are provided."""
    pa_table = pa.Table.from_pandas(DF)
    test_df_columns = pa_table.schema.names
    test_df_types = [
        trino.pyarrow_to_trino_type(str(typ)) for typ in pa_table.schema.types
    ]

    query = trino._create_table_query(
        schema_name=TEST_SCHEMA,
        table_name=TEST_TABLE,
        columns=test_df_columns,
        types=test_df_types,
        partition_cols=["country"],
    )

    expected_query = f"""
CREATE TABLE {TEST_SCHEMA}.{TEST_TABLE} (
    country VARCHAR,\n\tsales BIGINT
)
WITH (
    format = 'PARQUET',\n\tpartitioning = ARRAY['country']
)"""
    assert query == expected_query


def test__create_table_query_full(trino, DF):
    """Test create table query is construed as expected when partitions are provided."""
    pa_table = pa.Table.from_pandas(DF)
    test_df_columns = pa_table.schema.names
    test_df_types = [
        trino.pyarrow_to_trino_type(str(typ)) for typ in pa_table.schema.types
    ]

    query = trino._create_table_query(
        schema_name=TEST_SCHEMA,
        table_name=TEST_TABLE,
        columns=test_df_columns,
        types=test_df_types,
        format="ORC",
        partition_cols=["country"],
        sort_by_cols=["country"],
        location=TEST_TABLE_PATH,
    )

    expected_query = f"""
CREATE TABLE {TEST_SCHEMA}.{TEST_TABLE} (
    country VARCHAR,\n\tsales BIGINT
)
WITH (
    format = 'ORC',\n\tpartitioning = ARRAY['country'],\n\tsorted_by = ARRAY['country'],\n\tlocation = '{TEST_TABLE_PATH}'
)"""
    assert query == expected_query


def test_create_iceberg_table(trino, DF):
    # Assumptions.
    trino.create_iceberg_schema(TEST_SCHEMA, location=TEST_SCHEMA_PATH)
    schema_exists = trino._check_if_schema_exists(TEST_SCHEMA)
    table_exists = trino._check_if_table_exists(
        schema_name=TEST_SCHEMA, table_name=TEST_TABLE
    )
    assert schema_exists is True
    assert table_exists is False

    trino.create_iceberg_table(
        df=DF,
        schema_name=TEST_SCHEMA,
        table_name=TEST_TABLE,
    )

    table_exists = trino._check_if_table_exists(
        schema_name=TEST_SCHEMA, table_name=TEST_TABLE
    )
    assert table_exists is True

    # Cleanup.
    trino.drop_schema(TEST_SCHEMA, cascade=True)


def test_get_tables(trino, DF):
    # Assumptions.
    trino.create_iceberg_schema(TEST_SCHEMA, location=TEST_SCHEMA_PATH)
    schema_exists = trino._check_if_schema_exists(TEST_SCHEMA)
    assert schema_exists is True

    tables = trino.get_tables(TEST_SCHEMA)
    assert TEST_TABLE not in tables

    trino.create_iceberg_table(
        df=DF,
        schema_name=TEST_SCHEMA,
        table_name=TEST_TABLE,
    )
    table_exists = trino._check_if_table_exists(
        schema_name=TEST_SCHEMA, table_name=TEST_TABLE
    )
    assert table_exists is True

    # Test.
    tables = trino.get_tables(TEST_SCHEMA)
    assert TEST_TABLE in tables

    # Cleanup.
    trino.drop_schema(TEST_SCHEMA, cascade=True)


def test_drop_table(trino, DF):
    # Assumptions.
    trino.create_iceberg_schema(TEST_SCHEMA, location=TEST_SCHEMA_PATH)
    schema_exists = trino._check_if_schema_exists(TEST_SCHEMA)
    trino.create_iceberg_table(
        df=DF,
        schema_name=TEST_SCHEMA,
        table_name=TEST_TABLE,
    )
    table_exists = trino._check_if_table_exists(
        schema_name=TEST_SCHEMA, table_name=TEST_TABLE
    )

    assert schema_exists is True
    assert table_exists is True

    # Test.
    trino.drop_table(schema_name=TEST_SCHEMA, table_name=TEST_TABLE)
    table_exists = trino._check_if_table_exists(TEST_SCHEMA, TEST_TABLE)
    assert table_exists is False

    # Cleanup.
    trino.drop_schema(TEST_SCHEMA, cascade=True)
