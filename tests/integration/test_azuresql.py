import pyodbc
import pytest

from viadot.sources.azure_blob_storage import AzureBlobStorage
from viadot.sources.azure_sql import AzureSQL

SCHEMA = "sandbox"
TABLE = "test"


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
    except pyodbc.ProgrammingError:
        # in case tests end prematurely
        pass


def test_connection(azure_sql):
    azure_sql.con


def test_create_table(azure_sql):
    dtypes = {"country": "VARCHAR(100)", "sales": "FLOAT(24)"}
    result = azure_sql.create_table(
        schema=SCHEMA, table=TABLE, dtypes=dtypes, if_exists="replace"
    )
    assert result == True
    table_object_id = azure_sql.run(f"SELECT OBJECT_ID('{SCHEMA}.{TABLE}', 'U')")[0][0]
    assert table_object_id is not None


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
