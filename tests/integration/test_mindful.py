import os
import pytest
from unittest import mock
from viadot.sources import Mindful
from viadot.tasks import MindfulToCSV
from viadot.config import local_config

os.system("clear")

credentials_mindful = local_config["MINDFUL"]
header = {
    "Authorization": f"Bearer {credentials_mindful.get('VAULT')}",
}


class MockClass:
    status_code = 200
    content = b'[{"id":7277599,"survey_id":505,"phone_number":"","survey_type":"inbound"},{"id":7277294,"survey_id":504,"phone_number":"","survey_type":"web"}]'

    def json():
        test = [
            {
                "id": 7277599,
                "survey_id": 505,
                "phone_number": "",
                "survey_type": "inbound",
            },
            {"id": 7277294, "survey_id": 504, "phone_number": "", "survey_type": "web"},
        ]
        return test


class MockClass2:
    status_code = 204
    content = b""

    def json():
        return None


@pytest.mark.init
def test_instance_mindful():
    mf = Mindful(header=header)
    assert isinstance(mf, Mindful)


@mock.patch("viadot.sources.mindful.handle_api_response", return_value=MockClass)
@pytest.mark.connect
def test_mindful_api_response(mock_connection):
    mf = Mindful(header=header)
    mf.get_interactions_list()
    mf.get_responses_list()
    mock_connection.call_count == 2


@mock.patch("viadot.sources.mindful.handle_api_response", return_value=MockClass)
@pytest.mark.connect
def test_mindful_api_response2(mock_api_response):
    mf = Mindful(header=header)

    response = mf.get_interactions_list()

    assert response.status_code == 200 and isinstance(response.json(), list)
    assert mf.endpoint == "interactions"


@mock.patch("viadot.sources.mindful.handle_api_response", return_value=MockClass)
@pytest.mark.connect
def test_mindful_api_response3(mock_api_response):
    mf = Mindful(header=header)

    response = mf.get_responses_list()

    assert response.status_code == 200 and isinstance(response.json(), list)
    assert mf.endpoint == "responses"


@mock.patch("viadot.sources.mindful.handle_api_response", return_value=MockClass)
@pytest.mark.connect
def test_mindful_api_response4(mock_api_response):
    mf = Mindful(header=header)

    response = mf.get_survey_list()

    assert response.status_code == 200 and isinstance(response.json(), list)
    assert mf.endpoint == "surveys"


@mock.patch("viadot.sources.Mindful._mindful_api_response", return_value=MockClass)
@pytest.mark.save
def test_mindful_interactions(mock_connection):
    mf = Mindful(header=header)
    response = mf.get_interactions_list()
    mf.response_to_file(response)
    assert mf.endpoint == "interactions" and isinstance(mf.endpoint, str)

    assert os.path.exists("interactions.csv")
    os.remove("interactions.csv")


@mock.patch("viadot.sources.Mindful._mindful_api_response", return_value=MockClass)
@pytest.mark.save
def test_mindful_responses(mock_connection):
    mf = Mindful(header=header)
    response = mf.get_responses_list()
    mf.response_to_file(response)

    assert mf.endpoint == "responses" and isinstance(mf.endpoint, str)
    assert os.path.exists("responses.csv")
    os.remove("responses.csv")


@mock.patch("viadot.sources.Mindful._mindful_api_response", return_value=MockClass)
@pytest.mark.save
def test_mindful_surveys(mock_connection):
    mf = Mindful(header=header)
    response = mf.get_survey_list()
    mf.response_to_file(response)

    assert mf.endpoint == "surveys" and isinstance(mf.endpoint, str)
    assert os.path.exists("surveys.csv")
    os.remove("surveys.csv")


@mock.patch("viadot.sources.Mindful._mindful_api_response", return_value=MockClass2)
@pytest.mark.exception
def test_file_exception(mock_mindful):
    mf = MindfulToCSV()
    response = mf.run(credentials_mindful=credentials_mindful)
    assert response == None
