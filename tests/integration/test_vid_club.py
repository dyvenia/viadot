import pytest

from unittest import mock

from viadot.sources import VidClub


@pytest.fixture
def var_dictionary():
    variables = {}

    return variables


class MockClass:
    status_code = 200

    def json():
        test = {}
        return test


@pytest.mark.init
def test_create_club_class():
    vc = VidClub()
    assert vc


@pytest.mark.init
def test_default_credential_param():
    vc = VidClub()
    assert vc.credentials != None and type(vc.credentials) == dict


@pytest.mark.proper
def test_build_query():
    return


@pytest.mark.proper
def test_get_response():
    return
