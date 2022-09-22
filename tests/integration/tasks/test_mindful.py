import os
import pytest
from unittest import mock
from viadot.tasks import MindfulToCSV


class MockClass:
    status_code = 200
    content = b'[{"id":7277599,"survey_id":505,"phone_number":"","survey_type":"inbound"},{"id":7277294,"survey_id":504,"phone_number":"","survey_type":"web"}]'


@pytest.mark.init
def test_instance_mindful():
    mf = MindfulToCSV()
    assert isinstance(mf, MindfulToCSV)


@mock.patch("viadot.sources.mindful.handle_api_response", return_value=MockClass)
@pytest.mark.run
def test_mindful_run(mock_interactions):
    mf = MindfulToCSV()
    mf.run()
    mock_interactions.call_count == 2
    assert os.path.exists("interactions.csv")
    os.remove("interactions.csv")
    assert os.path.exists("responses.csv")
    os.remove("responses.csv")
