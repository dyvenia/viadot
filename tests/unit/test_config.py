import json
from pathlib import Path

import pytest

from viadot.config import Config, get_source_config, get_source_credentials


FAKE_SOURCE_CONFIG = {"fake_source": {"credentials": {"api_key": "test"}}}


@pytest.fixture
def TEST_CONFIG_PATH():
    """Creates and deletes a test config file for each test.

    Yields:
        config_path: The path to the test config file.
    """
    # Make sure we always create it from scratch.
    config_path = Path("config.yml")
    config_path.unlink(missing_ok=True)

    test_config = {"sources": [FAKE_SOURCE_CONFIG]}
    with Path(config_path).open("w") as f:
        json.dump(test_config, f)

    yield config_path

    # Cleanup after each test.
    config_path.unlink()


@pytest.fixture
def TEST_CONFIG_PATH_JSON():
    """Creates and deletes a test config file for each test.

    Yields:
        config_path: The path to the test config file.
    """
    # Make sure we always create it from scratch.
    config_path = Path("config.json")
    config_path.unlink(missing_ok=True)

    test_config = {"sources": [FAKE_SOURCE_CONFIG]}
    with Path(config_path).open("w") as f:
        json.dump(test_config, f)

    yield config_path

    # Cleanup after each test.
    config_path.unlink()


def test_config_from_yaml(TEST_CONFIG_PATH):
    config = Config.from_yaml(TEST_CONFIG_PATH)

    # Validate
    assert config is not None
    source_config = config.get("sources")[0]
    assert source_config == FAKE_SOURCE_CONFIG


def test_config_from_json(TEST_CONFIG_PATH_JSON):
    config = Config.from_json(TEST_CONFIG_PATH_JSON)

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
