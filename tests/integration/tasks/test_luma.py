from viadot.tasks import LumaIngest


def test_luma_ingest():
    task = LumaIngest(
        credentials_secret="luma-dev",
        metadata_dir_path="tests/resources/metadata/new",
    )
    task.run()
    assert True
