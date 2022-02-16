from typing import Any, Dict, List

from prefect import Flow, Task
from prefect.tasks.prefect import StartFlowRun


class StandardFlow(Flow):
    def __init__(
        self,
        name: str,
        project_name: str,
        extract_flow_names: str = None,
        load_flow_names: str = None,
        transform_conformed_flow_name: str = None,
        transfrom_operational_flow_name: str = None,
        *args: List[any],
        **kwargs: Dict[str, Any]
    ):
        self.extract_flow_names = extract_flow_names
        self.load_flow_names = load_flow_names
        self.transform_conformed_flow_name = transform_conformed_flow_name
        self.transfrom_operational_flow_name = transfrom_operational_flow_name
        self.project_name = project_name
        super().__init__(*args, name=name, **kwargs)
        self.gen_flow()

    def gen_start_flow_run_task(self, flow_name: str, flow: Flow = None) -> Task:
        flow_extract_run = StartFlowRun(wait=True)
        t = flow_extract_run.bind(
            flow_name=flow_name, project_name=self.project_name, flow=flow
        )
        return t

    def gen_flows_list(self):
        flow_list = [
            self.extract_flow_names,
            self.load_flow_names,
            self.transform_conformed_flow_name,
            self.transfrom_operational_flow_name,
        ]
        return list(filter(None, flow_list))

    def gen_flow(self) -> Flow:
        task_list = []
        flows_list = self.gen_flows_list()
        for index, flow_name in enumerate(flows_list):
            flow_task = self.gen_start_flow_run_task(
                flow_name=flow_name,
                flow=self,
            )
            if task_list:
                flow_task.set_upstream(task_list[index - 1], flow=self)
            task_list.append(flow_task)
