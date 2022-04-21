import pytest
import datetime

from viadot.tasks import PrefectLogs


@pytest.fixture(scope="session")
def expectation_suite():
    expectation_suite = {
        "data": {
            "flow": [
                {
                    "name": "3-operational cic_cisco_notready_time_cee ",
                    "version": 1,
                    "flow_runs": [
                        {
                            "id": "22d6eea3-10cd-4934-8134-668264b1609f",
                            "end_time": "2022-04-20T10:56:54.535481+00:00",
                            "start_time": "2022-04-20T10:55:55.663618+00:00",
                            "state": "Success",
                            "scheduled_start_time": "2022-04-20T10:55:22.070555+00:00",
                            "created_by_user_id": "98e01ccf-c42a-4fa0-a119-f9afa2188508",
                        }
                    ],
                },
                {"name": "3-operational cic_cisco_notready_time_nwe ", "flow_runs": []},
                {
                    "name": "3-operational cic_cisco_notready_time_nwe ",
                    "version": 1,
                    "flow_runs": [
                        {
                            "id": "48cc4803-f98f-4972-a4c4-2a149adab936",
                            "end_time": "2022-04-20T06:04:43.027992+00:00",
                            "start_time": "2022-04-20T06:00:47.943818+00:00",
                            "state": "Success",
                            "scheduled_start_time": "2022-04-20T06:00:00+00:00",
                            "created_by_user_id": null,
                        },
                        {
                            "id": "d78fcde7-213f-4bf0-a1cb-7b1dd27c0866",
                            "end_time": "2022-04-19T06:04:42.785552+00:00",
                            "start_time": "2022-04-19T06:00:28.199301+00:00",
                            "state": "Success",
                            "scheduled_start_time": "2022-04-19T06:00:00+00:00",
                            "created_by_user_id": null,
                        },
                    ],
                },
            ]
        }
    }

    yield expectation_suite


def test_prefect_logs(expectation_suite):
    run_date = datetime.date.today()

    flow = PrefectLogs(
        name="Extract prefect data test",
        scheduled_start_time="2022-04-19",
        filter_type="_gte",
        local_file_path=f"prefect_extract_logs_{run_date}.parquet",
        adls_path=f"raw/supermetrics/mp/prefect_extract_logs_{run_date}.parquet"
        # adls_sp_credentials_secret="App-Azure-CR-DatalakeGen2-AIA",
        # vault_name = "azuwevelcrkeyv001s",
    )

    results = flow.run()
    assert results.is_successful()
