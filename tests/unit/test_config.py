import json
from pathlib import Path

import yaml

from viadot.config import Config

TEST_CONFIG_PATH = Path("config.yml")


def test_config_from_yaml():
    # Assumptions.
    assert not TEST_CONFIG_PATH.exists()

    # Create fake config.
    fake_source_config = {"fake_surce": {"credentials": {}}}
    test_config = {"sources": [fake_source_config]}
    with open(TEST_CONFIG_PATH, "w") as f:
        yaml.dump(test_config, f)

    config = Config.from_yaml(TEST_CONFIG_PATH)

    # Validate
    assert config is not None
    source_config = config.get("sources")[0]
    assert source_config == fake_source_config

    # Cleaup.
    TEST_CONFIG_PATH.unlink()


def test_config_from_json():
    # Assumptions.
    assert not TEST_CONFIG_PATH.exists()

    # Create fake config.
    fake_source_config = {"fake_surce": {"credentials": {}}}
    test_config = {"sources": [fake_source_config]}
    with open(TEST_CONFIG_PATH, "w") as f:
        json.dump(test_config, f)

    config = Config.from_json(TEST_CONFIG_PATH)

    # Validate
    assert config is not None
    source_config = config.get("sources")[0]
    assert source_config == fake_source_config

    # Cleaup.
    TEST_CONFIG_PATH.unlink()
