from pandas import DataFrame
from prefect import flow
from prefect.runtime import flow_run
from prefect.settings import get_current_settings
from prefect.testing.utilities import prefect_test_harness
from pydantic import ValidationError
import pytest
import requests

from viadot.orchestration.prefect.tasks.failed_test_email_notification import (
    SmtpConfig,
    build_flow_run_url,
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


def test_build_flow_run_url():
    assert (
        build_flow_run_url(
            "12345678-1234-1234-1234-123456789012",
            "https://prefect.example.com/",
        )
        == "https://prefect.example.com/runs/flow-run/"
        "12345678-1234-1234-1234-123456789012"
    )


def test_build_flow_run_url_without_flow_run():
    assert build_flow_run_url(prefect_ui_url="https://prefect.example.com") is None


def test_build_flow_run_url_from_ephemeral_prefect_server():
    @flow
    def test_flow():
        settings = get_current_settings()
        return (
            build_flow_run_url(),
            settings.ui_url,
            settings.api.url,
            flow_run.id,
        )

    with prefect_test_harness():
        url, ui_url, api_url, flow_run_id = test_flow()
        response = requests.get(
            f"{api_url}/flow_runs/{flow_run_id}",
            timeout=10,
        )

    assert url is not None
    assert url.startswith(f"{ui_url}/runs/flow-run/")
    assert response.status_code == 200
    assert response.json()["id"] == str(flow_run_id)
