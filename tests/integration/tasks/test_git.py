from viadot.tasks import CloneRepo


def test_create_clone_repo():
    clone = CloneRepo(
        url="https://github.com/organization_name/repo_name.git",
        token="abc123",
    )
    assert isinstance(clone, CloneRepo)
    assert clone.token == "abc123"
