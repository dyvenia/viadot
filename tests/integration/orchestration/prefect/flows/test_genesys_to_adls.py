from viadot.orchestration.prefect.flows import genesys_to_adls


def test_genesys_to_adls():
    state = genesys_to_adls(
        azure_key_vault_secret="genesys-access-1",  # noqa: S106
        verbose=True,
        endpoint="routing_queues_members",
        post_data_list=[""],
        queues_ids=[
            "25e29c3e-ba33-4556-a78b-2abc40ec9699",
            "f4ef329a-d903-41f4-ad4d-876a753adf3c",
        ],
        drop_duplicates=True,
        adls_azure_key_vault_secret="app-azure-cr-datalakegen2",  # noqa: S106
        adls_path="raw/dyvenia_sandbox/genesys/genesys_agents.csv",
        adls_path_overwrite=True,
    )
    assert state.is_successful()
