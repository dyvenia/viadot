from datetime import datetime, timedelta, timezone
import struct

import pytest

from viadot.sources import SQLServer


@pytest.fixture
def sql_server():
    # Initialize the SQLServer instance with the test credentials.
    return SQLServer(config_key="sql_server")


def test_handle_datetimeoffset():
    # Example test data for a datetimeoffset value.
    dto_value = struct.pack("<6hI2h", 2021, 7, 3, 14, 30, 15, 123456000, 2, 0)
    expected_datetime = datetime(
        2021, 7, 3, 14, 30, 15, 123456, tzinfo=timezone(timedelta(hours=2))
    )

    result = SQLServer._handle_datetimeoffset(dto_value)
    assert result == expected_datetime


def test_schemas(sql_server):
    schemas = sql_server.schemas
    assert "dbo" in schemas  # Assuming 'dbo' schema exists in the test database.


def test_exists(sql_server):
    sql_server_table = sql_server.tables
    sample_table_schema = sql_server_table[0].split(".")
    sample_schema = sample_table_schema[0]
    sample_table = sample_table_schema[1]
    assert sql_server.exists(table=sample_table, schema=sample_schema)
