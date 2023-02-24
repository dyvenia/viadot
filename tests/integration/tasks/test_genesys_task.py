import pytest
from unittest import mock

from viadot.tasks import GenesysToCSV


@mock.patch.object(GenesysToCSV, "run")
@pytest.mark.to_csv
def test_genesys_to_csv(mock_reuturn):
    mock_reuturn.return_value = [
        "V_D_PROD_FB_QUEUE_CALLBACK.csv",
        "V_D_PROD_FB_QUEUE_CHAT.csv",
    ]
    genesys_to_csv = GenesysToCSV()
    files_name_list = genesys_to_csv.run()
    assert isinstance(files_name_list, list)
