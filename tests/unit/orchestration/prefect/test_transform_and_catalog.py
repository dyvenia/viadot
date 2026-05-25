import importlib
from pathlib import Path
from unittest.mock import MagicMock

import pytest


transform_and_catalog = importlib.import_module(
    "viadot.orchestration.prefect.flows.transform_and_catalog"
)


@pytest.fixture
def dbt_tasks(monkeypatch):
    tasks_by_name = {}

    def with_options(**options):
        task = MagicMock()
        future = MagicMock()
        future.result.return_value = []
        task.submit.return_value = future
        tasks_by_name[options["name"]] = task
        return task

    dbt_task = MagicMock()
    dbt_task.with_options.side_effect = with_options
    monkeypatch.setattr(transform_and_catalog, "dbt_task", dbt_task)

    return tasks_by_name


@pytest.mark.parametrize(
    "dbt_selects",
    [
        {"seed": "test_seed"},
        {"seed": "test_seed", "run": ""},
    ],
)
def test_run_dbt_transforms_seed_only_does_not_run_models(dbt_tasks, dbt_selects):
    transform_and_catalog._run_dbt_transforms(
        dbt_selects=dbt_selects,
        dbt_project_path_full=Path("dbt/lakehouse"),
        dbt_target="custom",
        fail_flow_only_on_build_failure=False,
        timeout_seconds=7200,
    )

    assert set(dbt_tasks) == {"dbt_seed"}
    dbt_tasks["dbt_seed"].submit.assert_called_once_with(
        project_path=Path("dbt/lakehouse"),
        command="seed -s test_seed -t custom",
    )


def test_run_dbt_transforms_can_seed_before_selected_run(dbt_tasks):
    transform_and_catalog._run_dbt_transforms(
        dbt_selects={
            "seed": "test_seed",
            "run": "test_model",
        },
        dbt_project_path_full=Path("dbt/lakehouse"),
        dbt_target="custom",
        fail_flow_only_on_build_failure=False,
        timeout_seconds=7200,
    )

    assert set(dbt_tasks) == {"dbt_seed", "dbt_run", "dbt_test"}
    dbt_tasks["dbt_seed"].submit.assert_called_once_with(
        project_path=Path("dbt/lakehouse"),
        command="seed -s test_seed -t custom",
    )
    dbt_tasks["dbt_run"].submit.assert_called_once_with(
        project_path=Path("dbt/lakehouse"),
        command="run -s test_model -t custom",
    )
    dbt_tasks["dbt_test"].submit.assert_called_once_with(
        project_path=Path("dbt/lakehouse"),
        command="test -s test_model -t custom",
        raise_on_failure=False,
    )
