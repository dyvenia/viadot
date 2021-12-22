import pyodbc
import pytest

from viadot.sources.azure_blob_storage import AzureBlobStorage
from viadot.sources.azure_sql import AzureSQL

SCHEMA = "sandbox"
TABLE = "test_azure_sql"
TABLE_2 = "test_azure_sql_2"


@pytest.fixture(scope="session")
def azure_sql(TEST_CSV_FILE_PATH, TEST_CSV_FILE_BLOB_PATH):

    # Upload the test file to Blob Storage.
    azstorage = AzureBlobStorage()
    azstorage.to_storage(
        from_path=TEST_CSV_FILE_PATH, to_path=TEST_CSV_FILE_BLOB_PATH, overwrite=True
    )

    azure_sql = AzureSQL(config_key="AZURE_SQL")

    yield azure_sql

    try:
        azure_sql.run(f"DROP TABLE {SCHEMA}.{TABLE}")
        azure_sql.run(f"DROP TABLE {SCHEMA}.{TABLE_2}")
    except pyodbc.ProgrammingError:
        # in case tests end prematurely
        pass


def test_connection(azure_sql):
    azure_sql.con


def test_table_exists(azure_sql):
    result = azure_sql.exists(table=TABLE, schema=SCHEMA)
    assert result == False


def test_create_table_replace(azure_sql):
    dtypes = {"country": "VARCHAR(100)", "sales": "FLOAT(24)"}
    result = azure_sql.create_table(
        schema=SCHEMA, table=TABLE, dtypes=dtypes, if_exists="replace"
    )
    assert result == True
    table_object_id = azure_sql.run(f"SELECT OBJECT_ID('{SCHEMA}.{TABLE}', 'U')")[0][0]
    assert table_object_id is not None


def test_create_table_delete(azure_sql, TEST_CSV_FILE_BLOB_PATH):
    insert_executed = azure_sql.bulk_insert(
        schema=SCHEMA,
        table=TABLE,
        source_path=TEST_CSV_FILE_BLOB_PATH,
        if_exists="replace",
    )
    assert insert_executed is True

    result = azure_sql.run(f"SELECT SUM(sales) FROM {SCHEMA}.{TABLE} AS total")
    assert int(result[0][0]) == 230

    table_object_id_insert = azure_sql.run(
        f"SELECT OBJECT_ID('{SCHEMA}.{TABLE}', 'U')"
    )[0][0]

    delete_executed = azure_sql.create_table(
        schema=SCHEMA, table=TABLE, if_exists="delete"
    )
    assert delete_executed is True

    table_object_id_delete = azure_sql.run(
        f"SELECT OBJECT_ID('{SCHEMA}.{TABLE}', 'U')"
    )[0][0]

    result = azure_sql.run(f"SELECT SUM(sales) FROM {SCHEMA}.{TABLE} AS total")
    assert result[0][0] is None
    assert table_object_id_insert == table_object_id_delete


def test_create_table_skip_1(azure_sql):
    """Test that if_exists = "skip" works when the table doesn't exist"""
    dtypes = {"country": "VARCHAR(100)", "sales": "FLOAT(24)"}
    result = azure_sql.create_table(
        schema=SCHEMA, table=TABLE_2, dtypes=dtypes, if_exists="skip"
    )
    assert result == True

    table_object_id = azure_sql.run(f"SELECT OBJECT_ID('{SCHEMA}.{TABLE}', 'U')")[0][0]
    assert table_object_id is not None


def test_create_table_skip_2(azure_sql):
    """Test that if_exists = "skip" works when the table exists"""
    dtypes = {"country": "VARCHAR(100)", "sales": "FLOAT(24)"}
    result = azure_sql.create_table(
        schema=SCHEMA, table=TABLE, dtypes=dtypes, if_exists="skip"
    )
    assert result == False


def test_bulk_insert(azure_sql, TEST_CSV_FILE_BLOB_PATH):

    azstorage = AzureBlobStorage()
    assert azstorage.exists(TEST_CSV_FILE_BLOB_PATH)

    executed = azure_sql.bulk_insert(
        schema=SCHEMA,
        table=TABLE,
        source_path=TEST_CSV_FILE_BLOB_PATH,
        if_exists="replace",
    )
    assert executed is True

    result = azure_sql.run(f"SELECT SUM(sales) FROM {SCHEMA}.{TABLE} AS total")
    assert int(result[0][0]) == 230


def test_schemas(azure_sql):
    results = azure_sql.schemas
    assert len(results) > 0


def test_tables(azure_sql):
    results = azure_sql.tables
    assert len(results) > 0
