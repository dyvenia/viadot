from pandas import DataFrame
from pydantic import ValidationError
import pytest

from viadot.orchestration.prefect.tasks.failed_test_email_notification import (
    SmtpConfig,
    find_column,
    find_model,
    find_schema,
    find_test,
)


@pytest.fixture
def failed_dbt_test_result():
    test_dict = {
        "status": "error",
        "timing": [
            {
                "name": "compile",
                "started_at": "2026-01-01T00:00:00.000000Z",
                "completed_at": "2026-01-01T00:00:00.000000Z",
            },
            {
                "name": "execute",
                "started_at": "2026-01-01T00:00:00.000000Z",
                "completed_at": "2026-01-01T00:00:00.000000Z",
            },
        ],
        "thread_id": "Thread-1 (worker)",
        "execution_time": 1.23,
        "message": 'Database Error in test model_test_name (models/gold/model/model.yml)\n  relation "schema.some_table" does not exist',
        "failures": None,
        "unique_id": "test.project.not_null_orders__customer_id.abc123def456",
        "compiled": True,
        "compiled_code": 'SELECT col1 FROM "devdb"."public"."orders" WHERE customer_id IS NOT NULL',
        "relation_name": '"devdb"."public"."orders"',
        "batch_results": None,
        "adapter_response._message": None,
        "adapter_response.rows_affected": None,
        "metadata.generated_at": "2026-01-01T00:00:00.000000Z",
    }

    return DataFrame(test_dict)


def test_find_schema(failed_dbt_test_result):
    result = failed_dbt_test_result["compiled_code"].apply(find_schema)
    assert result[0] == "public"


def test_find_column(failed_dbt_test_result):
    result = failed_dbt_test_result["unique_id"].apply(find_column)
    assert result[0] == "customer_id"


def test_find_model(failed_dbt_test_result):
    result = failed_dbt_test_result["compiled_code"].apply(find_model)
    assert result[0] == "orders"


def test_find_test(failed_dbt_test_result):
    result = failed_dbt_test_result["unique_id"].apply(
        find_test, test_types=["not_null"]
    )
    assert result[0] == "not_null"


def test_smtp_config_defaults():
    config = SmtpConfig(sender="test@gmail.com", password="secret")  # noqa: S106
    assert config.host == "smtp.gmail.com"
    assert config.port == 587


def test_smtp_config_custom_values():
    config = SmtpConfig(
        host="smtp.custom.com",
        port=465,
        sender="test@custom.com",
        password="secret",  # noqa: S106
    )
    assert config.host == "smtp.custom.com"
    assert config.port == 465


def test_smtp_config_missing_password():
    with pytest.raises(ValidationError):
        SmtpConfig(sender="test@gmail.com")  # type: ignore
