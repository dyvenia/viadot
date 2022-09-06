import pytest
from viadot.flows import PrefectLogs


@pytest.fixture(scope="session")
def expectation_suite():
    expectation_suite = {
        "data": {
            "project": [
                {
                    "id": "6f413380-e228-4d64-8e1b-41c6cd434a2a",
                    "name": "Installer Engagement",
                    "flows": [],
                },
                {
                    "id": "223a8acf-4cf0-4cf7-ae1f-b66f78e28813",
                    "name": "oso_reporting",
                    "flows": [
                        {
                            "id": "75a1211f-f5b1-44b7-b817-7ac28e95fba1",
                            "name": "1-raw google_analytics_oso_sps_gb extract",
                            "version": 5,
                            "flow_runs": [
                                {
                                    "id": "d26a1ea0-09d7-4816-adcd-45262a13c81b",
                                    "scheduled_start_time": "2022-09-06T01:00:00+00:00",
                                    "start_time": "2022-09-06T01:10:21.245486+00:00",
                                    "end_time": "2022-09-06T01:10:49.705029+00:00",
                                    "state": "Success",
                                    "created_by_user_id": null,
                                },
                                {
                                    "id": "f99ba041-7df7-4492-840f-599911432dfb",
                                    "scheduled_start_time": "2022-09-05T01:00:00+00:00",
                                    "start_time": "2022-09-05T01:10:23.950137+00:00",
                                    "end_time": "2022-09-05T01:11:01.199018+00:00",
                                    "state": "Success",
                                    "created_by_user_id": null,
                                },
                            ],
                        },
                        {
                            "id": "d671eb75-7dc3-4995-a183-88490c8c1f4c",
                            "name": "1-raw google_analytics_oso_sps_gb extract",
                            "version": 4,
                            "flow_runs": [],
                        },
                        {
                            "id": "5ba44231-5008-4878-80fd-002ebae6e5fd",
                            "name": "1-raw google_analytics_oso_sps_gb extract",
                            "version": 3,
                            "flow_runs": [],
                        },
                        {
                            "id": "060c5e67-bbb6-48af-8fe8-f55920d1313b",
                            "name": "1-raw google_analytics_oso_sps_gb extract",
                            "version": 2,
                            "flow_runs": [],
                        },
                        {
                            "id": "2bc3a960-61c4-41e7-a5d9-780f97726ff1",
                            "name": "1-raw google_analytics_oso_sps_gb extract",
                            "version": 1,
                            "flow_runs": [],
                        },
                    ],
                },
                {
                    "id": "844372db-2d22-495d-a343-b8f8cbcf8963",
                    "name": "sap",
                    "flows": [],
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
                    "id": "e2a926e2-ec86-4900-a24e-330a44b6cb19",
                    "name": "cic_test",
                    "flows": [],
                },
                {
                    "id": "667d5026-2f01-452a-b6fe-5437ca833066",
                    "name": "cic_dev",
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
                    "id": "dd2ccc32-2163-4f55-a746-1dbc6b28aaa4",
                    "name": "Hyperlocal",
                    "flows": [],
                },
                {
                    "id": "7131c357-bad7-43cf-aabc-87f9cf045384",
                    "name": "Installer Segmentation",
                    "flows": [],
                },
                {
                    "id": "94a8b8bf-14fa-4b64-ab78-af1d332dedd4",
                    "name": "Marketing KPI",
                    "flows": [],
                },
                {
                    "id": "ebe0e5aa-4add-4440-8c1a-6f9c74eb29fe",
                    "name": "dev",
                    "flows": [],
                },
                {
                    "id": "b5d924b0-4116-479f-a8f5-e28f9a9051ca",
                    "name": "velux",
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
                                where : {name: {_eq: "1-raw google_analytics_oso_sps_gb extract"}}
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
        adls_path=f"raw/supermetrics/mp/prefect_extract_logs.parquet",
    )

    results = flow.run()
    assert results.is_successful()
