import pytest
from viadot.flows import Pipeline
from viadot.config import local_config
from viadot.flows import Pipeline


def test_flow_of_flows():
    extract_flows_names = [
        "1-raw Linkedin ads Global extract",
        "1-raw Facebook ads Global extract test append",
    ]
    transform_flows_names = ["2-conformed Linkedin ads", "2-conformed Hubspot"]
    flow = Pipeline(
        name="Trigger test lists",
        extract_flows_names=extract_flows_names,
        transform_flows_names=transform_flows_names,
        project_name="Marketing KPI",
    )

    result = flow.run()
    assert result.is_successful()

    task_results = result.result.values()
    assert all([task_result.is_successful() for task_result in task_results])
