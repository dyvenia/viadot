import contextlib
import os

import pandas as pd
import pytest

from viadot.utils import skip_test_on_missing_extra


try:
    from viadot.sources import S3, RedshiftSpectrum
except ImportError:
    skip_test_on_missing_extra(source_name="RedshiftSpectrum", extra="aws")


TEST_DF = pd.DataFrame(
    [
        [0, "A"],
        [1, "B"],
        [2, "C"],
    ],
    columns=["col1", "col2"],
)
TEST_DF = TEST_DF.astype({"col1": "Int32", "col2": "string"})

S3_BUCKET = os.environ.get("VIADOT_S3_BUCKET")
TEST_SCHEMA = "test_schema"
TEST_TABLE = "test_table"


@pytest.fixture(scope="session")
def redshift(redshift_config_key):
    redshift = RedshiftSpectrum(config_key=redshift_config_key)

    yield redshift

    with contextlib.suppress(Exception):
        redshift.drop_schema(TEST_SCHEMA, drop_glue_database=True)

    with contextlib.suppress(AttributeError):
        redshift._con.close()


@pytest.fixture(scope="session")
def s3(s3_config_key):
    return S3(config_key=s3_config_key)


def test_create_schema(redshift):
    # Assumptions.
    schema_exists = redshift._check_if_schema_exists(schema=TEST_SCHEMA)
    assert schema_exists is False

    # Test.
    redshift.create_schema(TEST_SCHEMA)
    schema_exists = redshift._check_if_schema_exists(schema=TEST_SCHEMA)
    assert schema_exists is True

    # Cleanup.
    redshift.drop_schema(TEST_SCHEMA, drop_glue_database=True)


def test__check_if_table_exists(redshift):
    # Assumptions.
    redshift.create_schema(TEST_SCHEMA)
    schema_exists = redshift._check_if_schema_exists(schema=TEST_SCHEMA)
    assert schema_exists is True

    table_exists = redshift._check_if_table_exists(
        schema=TEST_SCHEMA,
        table=TEST_TABLE,
    )
    assert table_exists is False

    # Test.
    redshift.from_df(
        df=TEST_DF,
        to_path=f"s3://{S3_BUCKET}/viadot/{TEST_SCHEMA}/{TEST_TABLE}",
        schema=TEST_SCHEMA,
        table=TEST_TABLE,
    )
    table_exists = redshift._check_if_table_exists(
        schema=TEST_SCHEMA,
        table=TEST_TABLE,
    )
    assert table_exists is True

    # Cleanup.
    redshift.drop_table(schema=TEST_SCHEMA, table=TEST_TABLE, remove_files=True)
    redshift.drop_schema(TEST_SCHEMA)


def test_from_df(redshift, s3):
    # Assumptions.
    table_exists = redshift._check_if_table_exists(
        schema=TEST_SCHEMA,
        table=TEST_TABLE,
    )
    assert table_exists is False

    folder_exists = s3.exists(
        path=f"s3://{S3_BUCKET}/viadot/{TEST_SCHEMA}/{TEST_TABLE}"
    )
    assert folder_exists is False

    redshift.create_schema(TEST_SCHEMA)
    schema_exists = redshift._check_if_schema_exists(schema=TEST_SCHEMA)
    assert schema_exists is True

    # Test.
    redshift.from_df(
        df=TEST_DF,
        to_path=f"s3://{S3_BUCKET}/viadot/{TEST_SCHEMA}/{TEST_TABLE}",
        schema=TEST_SCHEMA,
        table=TEST_TABLE,
    )
    table_exists = redshift._check_if_table_exists(
        schema=TEST_SCHEMA,
        table=TEST_TABLE,
    )

    assert table_exists is True

    folder_exists = s3.exists(
        path=f"s3://{S3_BUCKET}/viadot/{TEST_SCHEMA}/{TEST_TABLE}"
    )
    assert folder_exists is True

    # Cleanup.
    redshift.drop_table(schema=TEST_SCHEMA, table=TEST_TABLE, remove_files=True)
    redshift.drop_schema(TEST_SCHEMA, drop_glue_database=True)


def test_from_df_no_table_folder_in_to_path(redshift, s3):
    """Test that the table folder is created if it's not specified in `to_path`."""
    # Assumptions.
    table_exists = redshift._check_if_table_exists(
        schema=TEST_SCHEMA,
        table=TEST_TABLE,
    )
    assert table_exists is False

    folder_exists = s3.exists(
        path=f"s3://{S3_BUCKET}/viadot/{TEST_SCHEMA}/{TEST_TABLE}"
    )
    assert folder_exists is False

    redshift.create_schema(TEST_SCHEMA)
    schema_exists = redshift._check_if_schema_exists(schema=TEST_SCHEMA)
    assert schema_exists is True

    # Test.
    redshift.from_df(
        df=TEST_DF,
        to_path=f"s3://{S3_BUCKET}/viadot/{TEST_SCHEMA}",
        schema=TEST_SCHEMA,
        table=TEST_TABLE,
    )

    table_exists = redshift._check_if_table_exists(schema=TEST_SCHEMA, table=TEST_TABLE)
    assert table_exists is True

    folder_exists = s3.exists(
        path=f"s3://{S3_BUCKET}/viadot/{TEST_SCHEMA}/{TEST_TABLE}"
    )
    assert folder_exists is True

    # Cleanup.
    redshift.drop_table(schema=TEST_SCHEMA, table=TEST_TABLE, remove_files=True)
    redshift.drop_schema(TEST_SCHEMA, drop_glue_database=True)


def test_to_df(redshift):
    # Assumptions.
    redshift.create_schema(TEST_SCHEMA)
    schema_exists = redshift._check_if_schema_exists(schema=TEST_SCHEMA)
    assert schema_exists is True

    # Test.
    redshift.from_df(
        df=TEST_DF,
        to_path=f"s3://{S3_BUCKET}/viadot/{TEST_SCHEMA}/{TEST_TABLE}",
        schema=TEST_SCHEMA,
        table=TEST_TABLE,
    )

    df = redshift.to_df(
        schema=TEST_SCHEMA,
        table=TEST_TABLE,
    )
    assert df.equals(TEST_DF)

    # Cleanup.
    redshift.drop_table(schema=TEST_SCHEMA, table=TEST_TABLE, remove_files=True)
    redshift.drop_schema(TEST_SCHEMA, drop_glue_database=True)


def test_get_tables(redshift):
    # Assumptions.
    redshift.create_schema(TEST_SCHEMA)
    schema_exists = redshift._check_if_schema_exists(schema=TEST_SCHEMA)
    assert schema_exists is True

    tables = redshift.get_tables(schema=TEST_SCHEMA)
    assert TEST_TABLE not in tables

    # Test.
    redshift.from_df(
        df=TEST_DF,
        to_path=f"s3://{S3_BUCKET}/viadot/{TEST_SCHEMA}/{TEST_TABLE}",
        schema=TEST_SCHEMA,
        table=TEST_TABLE,
    )
    tables = redshift.get_tables(schema=TEST_SCHEMA)
    assert tables == [TEST_TABLE]

    # Cleanup.
    redshift.drop_table(schema=TEST_SCHEMA, table=TEST_TABLE, remove_files=True)
    redshift.drop_schema(TEST_SCHEMA, drop_glue_database=True)


def test_get_tables_incorrect_schema(redshift):
    """Verify that `get_tables()` returns an empty list if the schema doesn't exist."""
    # Assumptions.
    schema_exists = redshift._check_if_schema_exists("test_schema2")
    assert schema_exists is False

    # Test.
    tables = redshift.get_tables(schema="test_schema2")
    assert tables == []


def test_drop_table(redshift):
    # Assumptions.
    redshift.create_schema(TEST_SCHEMA)
    schema_exists = redshift._check_if_schema_exists(schema=TEST_SCHEMA)
    assert schema_exists is True

    redshift.from_df(
        df=TEST_DF,
        to_path=f"s3://{S3_BUCKET}/viadot/{TEST_SCHEMA}/{TEST_TABLE}",
        schema=TEST_SCHEMA,
        table=TEST_TABLE,
    )
    table_exists = redshift._check_if_table_exists(schema=TEST_SCHEMA, table=TEST_TABLE)
    assert table_exists is True

    # Test.
    redshift.drop_table(schema=TEST_SCHEMA, table=TEST_TABLE, remove_files=True)

    table_exists = redshift._check_if_table_exists(schema=TEST_SCHEMA, table=TEST_TABLE)
    assert table_exists is False

    # Cleanup.
    redshift.drop_schema(TEST_SCHEMA, drop_glue_database=True)


def test__is_spectrum_schema(redshift):
    # Prerequisite.
    redshift.create_schema(TEST_SCHEMA)

    # Test.
    assert not redshift._is_spectrum_schema("public")
    assert redshift._is_spectrum_schema(TEST_SCHEMA)

    # Cleanup.
    redshift.drop_schema(TEST_SCHEMA, drop_glue_database=True)
