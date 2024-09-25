from prefect import flow

from viadot.orchestration.prefect.tasks import dbt_task


def test_dbt_task():
    @flow
    def test_flow():
        return dbt_task()

    result = test_flow()
    assert result == "Hello, prefect-viadot!"
