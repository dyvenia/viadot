from prefect import Flow

from viadot.tasks import SQLServerToDF
from viadot.flows import SQLServerToParquet


def test_sql_server_to_parquet_flow():
    flow = SQLServerToParquet(
        name="test_flow",
        sql_query="SELECT * FROM your_table",
        local_file_path="test.parquet",
        if_exists="fail",
        timeout=3600,
    )
    flow.gen_flow()
    assert isinstance(flow, Flow)
    assert len(flow.tasks) == 3  # Number of tasks in the flow
    tasks = list(flow.tasks)

    assert isinstance(tasks[0], SQLServerToDF)
