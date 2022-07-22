import pytest
from viadot.flows import PrefectLogs


@pytest.fixture(scope="session")
def expectation_suite():
    expectation_suite = {
        "data": {
            "project": [
                {"id": "1d3c5246-61e5-4aff-a07b-4b74959a46e4", "name": "dev_cdl"},
                {"id": "e2a926e2-ec86-4900-a24e-330a44b6cb19", "name": "cic_test"},
                {"id": "667d5026-2f01-452a-b6fe-5437ca833066", "name": "cic_dev"},
            ],
            "flow": [
                {
                    "project_id": "1d3c5246-61e5-4aff-a07b-4b74959a46e4",
                    "name": "3-operational korea billing_data_to_sql",
                    "version": 20,
                    "flow_runs": [
                        {
                            "id": "f94a55f6-d1d7-4be5-b2a4-7600a820a256",
                            "end_time": "2022-07-14T12:15:11.011063+00:00",
                            "start_time": "2022-07-14T12:14:39.282656+00:00",
                            "state": "Cancelled",
                            "scheduled_start_time": "2022-07-14T12:14:21.258614+00:00",
                            "created_by_user_id": "c99c87c4-f680-496d-90fc-05dfaf08154b",
                        },
                        {
                            "id": "fdfd2233-a8f0-4bb3-946f-060f91a655df",
                            "end_time": "2022-07-14T11:51:39.635059+00:00",
                            "start_time": "2022-07-14T11:50:52.362002+00:00",
                            "state": "Failed",
                            "scheduled_start_time": "2022-07-14T11:50:30.687881+00:00",
                            "created_by_user_id": "c99c87c4-f680-496d-90fc-05dfaf08154b",
                        },
                    ],
                }
            ],
        }
    }

    yield expectation_suite


def test_prefect_logs(expectation_suite):

    flow = PrefectLogs(
        name="Extract prefect data test",
        scheduled_start_time="2022-07-10",
        filter_type="_gte",
        local_file_path=f"prefect_extract_logs.parquet",
        adls_path=f"raw/supermetrics/mp/prefect_extract_logs.parquet",
    )

    results = flow.run()
    assert results.is_successful()
