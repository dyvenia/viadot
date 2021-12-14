from typing import Any, Dict, List

from prefect import Flow, Task, apply_map
from prefect.tasks.prefect import StartFlowRun
from prefect.exceptions import PrefectException


class Pipeline(Flow):
    def __init__(
        self,
        name: str,
        project_name: str,
        extract_flows_names: List[str],
        transform_flow_names: List[str],
        *args: List[any],
        **kwargs: Dict[str, Any]
    ):
        self.extract_flows_names = extract_flows_names
        self.transform_flow_names = transform_flow_names
        self.project_name = project_name
        super().__init__(*args, name=name, **kwargs)
        self.gen_flow()

    def gen_start_flow_run_task(self, flow_name: str, flow: Flow = None) -> Task:
        flow_extract_run = StartFlowRun(wait=True)
        task = flow_extract_run.bind(
            flow_name=flow_name, project_name=self.project_name, flow=flow
        )
        return task

    def gen_flow(self) -> Flow:
        try:
            for extract in self.extract_flows_names:
                extract_single_flow_run = apply_map(
                    self.gen_start_flow_run_task, extract, flow=self
                )
        except PrefectException as e:
            msg = "Error while extracting flow"
            raise PrefectException(msg)
        else:
            for transform in self.transform_flow_names:
                flow_run = StartFlowRun(wait=True)
                transform_single_flow_run = flow_run.bind(
                    flow_name=transform,
                    project_name=self.project_name,
                    flow=self,
                )
