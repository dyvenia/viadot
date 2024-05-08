import pytest
from viadot.sources import CloudForCustomers
from unittest import mock


def test_credentials():
    endpoint = "ServiceRequestCollection"
    c4c = CloudForCustomers(endpoint=endpoint, params={"$top": "2"})
    df = c4c.to_df(
        fields=["ProductRecipientPartyName", "CreationDateTime", "CreatedBy"]
    )
    assert df.empty == False


class MockResponse:
    def __init__(
        self,
    ):
        self.json_data = {"d": {"results": []}}
        self.status_code = 200

    def json(self):
        return self.json_data


@mock.patch(
    "viadot.sources.CloudForCustomers.get_response", return_value=MockResponse()
)
def test_c4c_empty_response(mock_get):
    with pytest.raises(
        ValueError,
        match="Response from the cloud for customers for specified parameters is empty!",
    ):
        endpoint = "ServiceRequestCollection"
        c4c = CloudForCustomers(endpoint=endpoint, params={"$top": "2"})
        df = c4c.to_df(
            fields=["ProductRecipientPartyName", "CreationDateTime", "CreatedBy"]
        )
