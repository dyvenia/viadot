from pathlib import Path

import pytest

from viadot.config import (
    Config,
    _find_project_config,
    _load_default_config,
    get_max_batch_size,
    get_source_config,
    get_source_credentials,
)


FAKE_SOURCE_CONFIG = {"fake_source": {"credentials": {"api_key": "test"}}}


@pytest.fixture
def TEST_CONFIG_PATH():
    """Creates and deletes a test config file for each test.

    Yields:
        config_path: The path to the test config file.
    """
    # Make sure we always create it from scratch.
    config_path = Path("viadot.yml")
    config_path.unlink(missing_ok=True)

    config_path.write_text(
        "sources:\n  - fake_source:\n      credentials:\n        api_key: test\n"
    )

    yield config_path

    # Cleanup after each test.
    config_path.unlink()


def test_config_from_yaml(TEST_CONFIG_PATH):
    config = Config.from_yaml(TEST_CONFIG_PATH)

    # Validate
    assert config is not None
    source_config = config.get("sources")[0]
    assert source_config == FAKE_SOURCE_CONFIG


def test_get_source_config(TEST_CONFIG_PATH):
    config = Config.from_yaml(TEST_CONFIG_PATH)

    # Validate
    source_config = get_source_config("fake_source", config=config)
    assert source_config == FAKE_SOURCE_CONFIG["fake_source"]


def test_get_source_credentials(TEST_CONFIG_PATH):
    config = Config.from_yaml(TEST_CONFIG_PATH)

    # Validate
    credentials = get_source_credentials("fake_source", config=config)
    assert credentials == FAKE_SOURCE_CONFIG["fake_source"]["credentials"]


def test_get_max_batch_size_uses_fallback():
    assert get_max_batch_size(Config()) == 20_000


def test_get_max_batch_size_uses_configured_value():
    config = Config(settings={"max_batch_size": 5_000})

    assert get_max_batch_size(config) == 5_000


def test_default_config_prefers_project_directory(tmp_path, monkeypatch):
    project_dir = tmp_path / "project"
    home_dir = tmp_path / "home" / ".config" / "viadot"
    project_dir.mkdir()
    home_dir.mkdir(parents=True)

    (project_dir / "viadot.yaml").write_text("settings:\n  max_batch_size: 1000\n")
    (home_dir / "viadot.yaml").write_text("settings:\n  max_batch_size: 2000\n")
    monkeypatch.chdir(project_dir)

    import viadot.config as config_module

    monkeypatch.setattr(config_module, "USER_HOME", home_dir.parent.parent)

    assert _load_default_config()["settings"]["max_batch_size"] == 1000


def test_default_config_falls_back_to_home_directory(tmp_path, monkeypatch):
    project_dir = tmp_path / "project"
    home_dir = tmp_path / "home" / ".config" / "viadot"
    project_dir.mkdir()
    home_dir.mkdir(parents=True)

    (home_dir / "viadot.yaml").write_text("settings:\n  max_batch_size: 2000\n")
    monkeypatch.chdir(project_dir)

    import viadot.config as config_module

    monkeypatch.setattr(config_module, "USER_HOME", home_dir.parent.parent)

    assert _load_default_config()["settings"]["max_batch_size"] == 2000


def test_project_config_is_found_in_a_parent_directory(tmp_path, monkeypatch):
    project_dir = tmp_path / "project"
    nested_dir = project_dir / "src" / "package"
    nested_dir.mkdir(parents=True)
    (project_dir / "viadot.yaml").write_text("settings:\n  max_batch_size: 3000\n")
    monkeypatch.chdir(nested_dir)

    assert _find_project_config()["settings"]["max_batch_size"] == 3000


def test_nearest_project_config_takes_precedence(tmp_path, monkeypatch):
    project_dir = tmp_path / "project"
    nested_dir = project_dir / "src"
    nested_dir.mkdir(parents=True)
    (project_dir / "viadot.yaml").write_text("settings:\n  max_batch_size: 3000\n")
    (nested_dir / "viadot.yaml").write_text("settings:\n  max_batch_size: 4000\n")
    monkeypatch.chdir(nested_dir)

    assert _find_project_config()["settings"]["max_batch_size"] == 4000
