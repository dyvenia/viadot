import json
import os
import pytest
from os.path import expanduser, join
from viadot.config import Config

LOCAL_CREDENTIALS_PATH = "/home/viadot/tests/unit/credentials.json"
USER_HOME = expanduser("~")

def get_credentials(key: str):
    with open(LOCAL_CREDENTIALS_PATH, "r") as f:
        credentials = json.load(f)
    return credentials[key]

def test_credentials():
    test_credentials = Config.from_json(LOCAL_CREDENTIALS_PATH, key="TEST_STANZA")
    credentials = get_credentials("TEST_STANZA")
    assert test_credentials == credentials

@pytest.fixture(scope="session")
def home_credentials():
    HOME_CREDENTIALS_PATH = join(USER_HOME, ".config", "credentials.json")
    yield HOME_CREDENTIALS_PATH
    os.rename(HOME_CREDENTIALS_PATH + "_renamed", HOME_CREDENTIALS_PATH)

def test_no_credentials_file(home_credentials):
    os.rename(home_credentials, home_credentials + "_renamed")
    from viadot.sources import UKCarbonIntensity
    ukci = UKCarbonIntensity()

