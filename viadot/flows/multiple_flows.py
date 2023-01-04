from typing import Any, Dict, List

from prefect import Flow, task
from prefect.tasks.prefect import create_flow_run, wait_for_flow_run
from prefect.utilities import logging

logger = logging.get_logger()


@task(timeout=3600)
def run_flows_list(flow_name: str, flows_list: List[List] = [List[None]]):
    """
    Task for running multiple flows in the given order. Task will create flow of flows.
    Args:
        flow_name(str): Name of a new flow.
        flows_list(List[List]): List containing lists of flow names and project names - [["flow1_name" , "project_name"], ["flow2_name" , "project_name"]].
            Flows have to be in the correct oreder. Defaults to [List[None]].
    """
    with Flow(flow_name) as flow:
        for i in range(len(flows_list)):
            exec(
                f"flow_{i}= create_flow_run(flow_name=flows_list[i][0], project_name=flows_list[i][1])"
            )
            exec(
                f"wait_for_flow_{i} = wait_for_flow_run(flow_{i}, raise_final_state=True)"
            )
        for i in range(1, len(flows_list)):
            exec(f"flow_{i}.set_upstream(wait_for_flow_{i-1})")
        flow_state = flow.run()
        if flow_state.is_failed():
            logger.error("One of the flows has failed!")
            raise ValueError("One of the flows has failed!")
        else:
            logger.info("All of the tasks succeeded.")


class MultipleFlows(Flow):
    """Flow to run multiple flows in the given order.
    Args:
        flow_name(str): Name of a new flow.
        flows_list(List[List]): List containing lists of flow names and project names - [["flow1_name" , "project_name"], ["flow2_name" , "project_name"]].
            Flows have to be in the correct oreder. Defaults to [List[None]].
    """

    def __init__(
        self,
        name: str,
        flows_list: List[List] = [List[None]],
        *args: List[any],
        **kwargs: Dict[str, Any],
    ):
        self.flows_list = flows_list
        super().__init__(*args, name=name, **kwargs)
        self.gen_flow()

    def gen_flow(self) -> Flow:
        run_flows_list.bind(
            flow_name=self.name,
            flows_list=self.flows_list,
            flow=self,
        )
