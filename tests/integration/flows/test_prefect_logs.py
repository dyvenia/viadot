import pytest

from viadot.flows import PrefectLogs


@pytest.fixture(scope="session")
def expectation_suite():
    expectation_suite = {
        "data": {
            "project": [
                {
                    "id": "223a8acf-4cf0-4cf7-ae1f-b66f78e28813",
                    "name": "Admin",
                    "flows": [
                        {
                            "id": "b13dcc6d-b621-4acd-88be-2cf28715a7c5",
                            "name": "1-raw table_catalog extract",
                            "version": 3,
                            "flow_runs": [
                                {
                                    "id": "d1d76bbd-b494-4cf2-bf59-3dfccf520039",
                                    "scheduled_start_time": "2022-09-06T09:19:47.937928+00:00",
                                    "start_time": "2022-09-06T09:20:06.944586+00:00",
                                    "end_time": "2022-09-06T09:20:39.386856+00:00",
                                    "state": "Cancelled",
                                    "created_by_user_id": "5878be75-ee66-42f4-8179-997450063ea4",
                                }
                            ],
                        },
                        {
                            "id": "14b1a89e-f902-48a1-b6df-43cacdb91e1a",
                            "name": "1-raw table_catalog extract",
                            "version": 2,
                            "flow_runs": [],
                        },
                        {
                            "id": "a1eace09-38b4-46bf-bacf-a5d29bdbb633",
                            "name": "1-raw table_catalog extract",
                            "version": 1,
                            "flow_runs": [],
                        },
                    ],
                },
                {
                    "id": "512d0f29-2ceb-4177-b7d8-c5908da666ef",
                    "name": "integrations",
                    "flows": [],
                },
                {
                    "id": "1d3c5246-61e5-4aff-a07b-4b74959a46e4",
                    "name": "dev_cdl",
                    "flows": [],
                },
                {
                    "id": "eac9b6d4-725a-4354-bf8f-25e7828ea2d8",
                    "name": "Admin",
                    "flows": [],
                },
                {
                    "id": "52217a3c-f42a-4448-afc4-2a325415b8e8",
                    "name": "test",
                    "flows": [],
                },
                {
                    "id": "516b3fe9-1c26-47a4-b797-e6a77bee390c",
                    "name": "cic",
                    "flows": [],
                },
                {
                    "id": "ebe0e5aa-4add-4440-8c1a-6f9c74eb29fe",
                    "name": "dev",
                    "flows": [],
                },
            ]
        }
    }

    yield expectation_suite


def test_prefect_logs(expectation_suite):
    flow = PrefectLogs(
        name="Extract prefect data test",
        query="""
                {
                        project {
                            id
                            name
                            flows (
                                where : {name: {_eq: "1-raw table_catalog extract"}}
                            ) {
                                    id
                                    name
                                    version
                                    flow_runs(
                                        order_by: {end_time: desc}
                                        where: {_and:
                                            [
                                            {scheduled_start_time:{ %s: "%s" }},
                                            {state: {_neq: "Scheduled"}}
                                            ]
                                        }
                                        )
                                            {
                                            id
                                            scheduled_start_time
                                            start_time
                                            end_time
                                            state
                                            created_by_user_id
                                            }
                            }
                        }
                    }
        """,
        scheduled_start_time="2022-09-05",
        filter_type="_gte",
        local_file_path=f"prefect_extract_logs.parquet",
        adls_path=f"raw/tests/prefect_extract_logs.parquet",
    )

    results = flow.run()
    assert results.is_successful()
