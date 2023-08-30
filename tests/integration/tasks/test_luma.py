from viadot.tasks import LumaIngest


def test_luma_ingest():
    luma_task = LumaIngest(
        name="Luma test",
        metadata_dir_path="tests/resources/metadata/new",
        url="www.luma_url.com",
        dbt_project_path="project_path",
        credentials_secret="luma-dev",
    )
    assert luma_task.name == "Luma test"
    assert isinstance(luma_task, LumaIngest)
