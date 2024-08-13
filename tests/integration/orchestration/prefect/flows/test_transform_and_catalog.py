from viadot.orchestration.prefect.flows import transform_and_catalog


def test_transform_and_catalog_model(dbt_repo_url, LUMA_URL):
    logs = transform_and_catalog(
        dbt_repo_url=dbt_repo_url,
        dbt_repo_branch="luma",
        dbt_project_path="dbt_luma",
        dbt_target="dev",
        luma_url=LUMA_URL,
        metadata_kind="model",
    )
    log = "\n".join(logs)
    success_message = "The request was successful!"

    assert success_message in log


def test_transform_and_catalog_model_run(dbt_repo_url, LUMA_URL):
    logs = transform_and_catalog(
        dbt_repo_url=dbt_repo_url,
        dbt_repo_branch="luma",
        dbt_project_path="dbt_luma",
        dbt_target="dev",
        luma_url=LUMA_URL,
        metadata_kind="model_run",
    )
    log = "\n".join(logs)
    success_message = "The request was successful!"

    assert success_message in log
