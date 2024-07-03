import pytest
from datetime import datetime, timedelta, timezone
import struct

from viadot.sources import SQLServer 


@pytest.fixture
def sql_server():
    # Initialize the SQLServer instance with the test credentials
    sql_server = SQLServer(config_key = "sql_server")
    yield sql_server


def test_handle_datetimeoffset():
    # Example test data for a datetimeoffset value
    dto_value = struct.pack("<6hI2h", 2021, 7, 3, 14, 30, 15, 123456000, 2, 0)
    expected_datetime = datetime(2021, 7, 3, 14, 30, 15, 123456, tzinfo=timezone(timedelta(hours=2)))
    
    result = SQLServer._handle_datetimeoffset(dto_value)
    assert result == expected_datetime

def test_schemas(sql_server):
    schemas = sql_server.schemas
    assert "dbo" in schemas  # Assuming 'dbo' schema exists in the test database


