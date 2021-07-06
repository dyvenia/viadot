import json
import os
from os.path import dirname, expanduser, join, realpath

import pytest

from viadot.config import Config

USER_HOME = expanduser("~")
UNIT_TESTS_DIR_PATH = dirname(realpath(__file__))
UNIT_TESTS_CONFIG_PATH = join(UNIT_TESTS_DIR_PATH, "credentials.json")
DEFAULT_CONFIG_PATH = join(USER_HOME, ".config", "credentials.json")


@pytest.fixture(scope="session")
def config():
    CONFIG_PATH = os.environ.get("VIADOT_CONFIG_PATH") or DEFAULT_CONFIG_PATH
    yield CONFIG_PATH
    os.rename(CONFIG_PATH + "_renamed", CONFIG_PATH)


def get_credentials(key: str):
    with open(UNIT_TESTS_CONFIG_PATH, "r") as f:
        credentials = json.load(f)
    return credentials[key]


def test_credentials():
    test_credentials = Config.from_json(UNIT_TESTS_CONFIG_PATH, key="TEST_STANZA")
    credentials = get_credentials("TEST_STANZA")
    assert test_credentials == credentials


def test_no_credentials_file(config):
    os.rename(config, config + "_renamed")
    from viadot.sources import UKCarbonIntensity

    UKCarbonIntensity()
