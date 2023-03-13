import pytest
from unittest import mock
from datetime import datetime, timedelta

from viadot.tasks import GenesysToCSV


@pytest.fixture
def var_dictionary() -> None:
    """Function where variables are stored."""

    variables = {
        "post_data_list": [
            {
                "name": "AGENT_STATUS_DETAIL_VIEW",
                "timeZone": "UTC",
                "exportFormat": "CSV",
                "interval": "2023-03-01T00:00:00/2023-03-02T00:00:00",
                "period": "PT30M",
                "viewType": "AGENT_STATUS_DETAIL_VIEW",
                "filter": {"userIds": ["9eb0fe4e-937e-4443-a5a4-d1b5dbd76520"]},
                "read": True,
                "locale": "en-us",
                "hasFormatDurations": False,
                "hasSplitFilters": True,
                "excludeEmptyRows": True,
                "hasSummaryRow": False,
                "csvDelimiter": "COMMA",
                "hasCustomParticipantAttributes": True,
            }
        ]
    }
    return variables


class MockGenesysTask:
    report_data = [[None, "COMPLETED"], [None, "COMPLETED"]]

    def genesys_generate_exports(post_data_list):
        pass

    def get_reporting_exports_data():
        pass

    def delete_all_reporting_exports():
        pass

    def download_all_reporting_exports(path):
        return [
            "V_D_PROD_FB_QUEUE_CALLBACK.csv",
            "V_D_PROD_FB_QUEUE_CHAT.csv",
        ]

    def delete_all_reporting_exports():
        pass


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


@mock.patch("viadot.tasks.genesys.Genesys", return_value=MockGenesysTask)
@pytest.mark.files
def test_genesys_files_type(mock_genesys, var_dictionary):
    to_csv = GenesysToCSV()
    file_name = to_csv.run(
        view_type="agent_status_detail_view",
        view_type_time_sleep=5,
        post_data_list=var_dictionary["post_data_list"],
        start_date=datetime.now().strftime("%Y-%m-%d"),
    )
    mock_genesys.assert_called_once()
    assert len(file_name) > 1
