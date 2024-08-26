from viadot.orchestration.prefect.tasks import bcp


SCHEMA = "sandbox"
TABLE = "test_bcp"
ERROR_TABLE = "test_bcp_error"
ERROR_LOG_FILE = "log_file.log"
TEST_CSV_FILE_PATH = "test_bcp.csv"


def test_bcp():
    try:
        result = bcp(
            credentials_secret="sql-server",  # noqa: S106
            path=TEST_CSV_FILE_PATH,
            schema=SCHEMA,
            table=TABLE,
            error_log_file_path=ERROR_LOG_FILE,
        )
    except Exception:
        result = False
    assert result is not False
