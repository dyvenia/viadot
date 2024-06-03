from orchestration.prefect_viadot.tasks import dbt_task
from prefect import flow


def test_dbt_task():
    @flow
    def test_flow():
        return dbt_task()

    result = test_flow()
    assert result == "Hello, prefect-viadot!"
