"""'test_mediatool.py'."""

import json
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from viadot.exceptions import CredentialError
from viadot.sources import Mediatool
from viadot.sources.mediatool import MediatoolCredentials


variables = {
    "credentials": {"user_id": "test_user", "token": "test_token"},
    "organizations": {
        "organizations": [{"_id": "1", "name": "Org1", "abbreviation": "O1"}]
    },
    "media_entries": {
        "mediaEntries": [
            {
                "_id": "1",
                "campaignId": "1",
                "organizationId": "1",
                "vehicleId": "1",
                "mediaTypeId": "1",
                "name": "Entry1",
                "customCol1": "CustomValue1",
                "customCol2": "CustomValue2",
            }
        ]
    },
    "vehicle": {"vehicle": {"_id": "1", "name": "Vehicle1"}},
    "campaigns": {
        "campaigns": [
            {"_id": "1", "name": "Campaign1", "conventionalName": "conventionalName1"}
        ]
    },
    "media_types": {"mediaType": {"_id": "1", "name": "Type1", "type": "Type"}},
}


@pytest.fixture
def mock_multiple_api_responses():
    """Fixture to mock multiple API responses for Mediatool.to_df() method."""
    mock_organizations_response = MagicMock()
    mock_organizations_response.text = json.dumps(variables["organizations"])

    mock_media_entries_response = MagicMock()
    mock_media_entries_response.text = json.dumps(variables["media_entries"])

    mock_vehicle_response = MagicMock()
    mock_vehicle_response.text = json.dumps(variables["vehicle"])

    mock_campaigns_response = MagicMock()
    mock_campaigns_response.text = json.dumps(variables["campaigns"])

    mock_media_types_response = MagicMock()
    mock_media_types_response.text = json.dumps(variables["media_types"])

    return [
        mock_organizations_response,
        mock_media_entries_response,
        mock_vehicle_response,
        mock_campaigns_response,
        mock_media_types_response,
    ]


@pytest.mark.basic
def test_mediatool_credentials():
    """Test Mediatool credentials."""
    MediatoolCredentials(user_id="test_user", token="test_token")  # noqa: S106


@pytest.mark.basic
@patch("viadot.sources.mediatool.get_source_credentials", return_value=None)
def test_missing_credentials(mock_get_source_credentials):
    """Test raise error without credentials."""
    with pytest.raises(CredentialError):
        Mediatool()

    mock_get_source_credentials.assert_called_once()


@pytest.mark.functions
@patch("viadot.sources.mediatool.handle_api_response")
def test_get_organizations(mock_handle_api_response):
    """Test Mediatool `_get_organizations` function."""
    mock_response = MagicMock()
    mock_response.text = json.dumps(variables["organizations"])
    mock_handle_api_response.return_value = mock_response

    mediatool = Mediatool(credentials=variables["credentials"])

    result = mediatool._get_organizations(user_id="test_user")
    expected_result = [{"_id": "1", "name": "Org1", "abbreviation": "O1"}]
    assert result == expected_result


@pytest.mark.functions
@patch("viadot.sources.mediatool.handle_api_response")
def test_get_media_entries(mock_handle_api_response):
    """Test Mediatool `_get_media_entries` function."""
    mock_response = MagicMock()
    mock_response.text = json.dumps(variables["media_entries"])
    mock_handle_api_response.return_value = mock_response

    mediatool = Mediatool(credentials=variables["credentials"])

    result = mediatool._get_media_entries(organization_id="org_id")
    expected_result = [
        {
            "_id": "1",
            "campaignId": "1",
            "organizationId": "1",
            "vehicleId": "1",
            "mediaTypeId": "1",
            "name": "Entry1",
            "customCol1": "CustomValue1",
            "customCol2": "CustomValue2",
        }
    ]
    assert result == expected_result


@pytest.mark.functions
@patch("viadot.sources.mediatool.handle_api_response")
def test_get_vehicles(mock_handle_api_response):
    """Test Mediatool `_get_vehicles` function."""
    mock_response = MagicMock()
    mock_response.text = json.dumps(variables["vehicle"])
    mock_handle_api_response.return_value = mock_response

    mediatool = Mediatool(credentials=variables["credentials"])

    result = mediatool._get_vehicles(vehicle_ids=["1"])
    expected_result = [{"_id": "1", "name": "Vehicle1"}]
    assert result == expected_result


@pytest.mark.functions
@patch("viadot.sources.mediatool.handle_api_response")
def test_get_campaigns(mock_handle_api_response):
    """Test Mediatool `_get_campaigns` function."""
    mock_response = MagicMock()
    mock_response.text = json.dumps(variables["campaigns"])
    mock_handle_api_response.return_value = mock_response

    mediatool = Mediatool(credentials=variables["credentials"])

    result = mediatool._get_campaigns(organization_id="org_id")
    expected_result = [
        {"_id": "1", "name": "Campaign1", "conventionalName": "conventionalName1"}
    ]
    assert result == expected_result


@pytest.mark.functions
@patch("viadot.sources.mediatool.handle_api_response")
def test_get_media_types(mock_handle_api_response):
    """Test Mediatool `_get_media_types` function."""
    mock_response = MagicMock()
    mock_response.text = json.dumps(variables["media_types"])
    mock_handle_api_response.return_value = mock_response

    mediatool = Mediatool(credentials=variables["credentials"])

    result = mediatool._get_media_types(media_type_ids=["1"])
    expected_result = [{"_id": "1", "name": "Type1", "type": "Type"}]
    assert result == expected_result


@pytest.mark.connect
@patch("viadot.sources.mediatool.handle_api_response")
def test_to_records(mock_handle_api_response):
    """Test Mediatool `_to_records` method."""
    mock_response = MagicMock()
    mock_response.text = json.dumps(variables["organizations"])
    mock_handle_api_response.return_value = mock_response

    mediatool = Mediatool(credentials=variables["credentials"])

    result = mediatool._to_records(endpoint="organizations")
    expected_result = [{"_id": "1", "name": "Org1", "abbreviation": "O1"}]
    assert result == expected_result


@pytest.mark.functions
@patch("viadot.sources.mediatool.handle_api_response")
def test_fetch_and_transform(mock_handle_api_response):
    """Test Mediatool `to_df` method."""
    mock_response = MagicMock()
    mock_response.text = json.dumps({"mediaEntries": [{"_id": "1", "name": "Entry1"}]})
    mock_handle_api_response.return_value = mock_response

    mediatool = Mediatool(credentials=variables["credentials"])

    result_df = mediatool.fetch_and_transform(endpoint="media_entries")

    expected_result = pd.DataFrame(
        {"_id_media_entries": ["1"], "name_media_entries": ["Entry1"]}
    )
    pd.testing.assert_frame_equal(result_df, expected_result)


@pytest.mark.functions
@patch("viadot.sources.mediatool.handle_api_response")
def test_to_df(mock_handle_api_response, mock_multiple_api_responses):
    """Test Mediatool `to_df` method."""
    mock_handle_api_response.side_effect = mock_multiple_api_responses

    mediatool = Mediatool(credentials=variables["credentials"])

    result_df = mediatool.to_df(organization_ids=["1"])
    result_df.drop(
        columns=["_viadot_source", "_viadot_downloaded_at_utc"],
        inplace=True,
        axis=1,
    )
    expected_result = pd.DataFrame(
        {
            "_id": ["1"],
            "campaignId": ["1"],
            "organizationId": ["1"],
            "vehicleId": ["1"],
            "mediaTypeId": ["1"],
            "name": ["Entry1"],
            "customCol1": "CustomValue1",
            "customCol2": "CustomValue2",
            "_id_organizations": ["1"],
            "name_organizations": ["Org1"],
            "abbreviation_organizations": ["O1"],
            "_id_campaigns": ["1"],
            "name_campaigns": ["Campaign1"],
            "conventionalName_campaigns": ["conventionalName1"],
            "_id_vehicles": ["1"],
            "name_vehicles": ["Vehicle1"],
            "_id_media_types": ["1"],
            "name_media_types": ["Type1"],
        }
    )

    pd.testing.assert_frame_equal(result_df, expected_result)


@pytest.mark.functions
@patch("viadot.sources.mediatool.handle_api_response")
def test_to_df_media_entries_columns(
    mock_handle_api_response, mock_multiple_api_responses
):
    """Test Mediatool `to_df` method, with optional media_entries_columns."""
    mock_handle_api_response.side_effect = mock_multiple_api_responses

    mediatool = Mediatool(credentials=variables["credentials"])

    result_df = mediatool.to_df(
        organization_ids=["1"],
        media_entries_columns=[
            "_id",
            "campaignId",
            "organizationId",
            "vehicleId",
            "mediaTypeId",
            "name",
        ],
    )
    result_df.drop(
        columns=["_viadot_source", "_viadot_downloaded_at_utc"],
        inplace=True,
        axis=1,
    )
    expected_result = pd.DataFrame(
        {
            "_id": "1",
            "campaignId": "1",
            "organizationId": "1",
            "vehicleId": "1",
            "mediaTypeId": "1",
            "name": "Entry1",
            "_id_organizations": ["1"],
            "name_organizations": ["Org1"],
            "abbreviation_organizations": ["O1"],
            "_id_campaigns": ["1"],
            "name_campaigns": ["Campaign1"],
            "conventionalName_campaigns": ["conventionalName1"],
            "_id_vehicles": ["1"],
            "name_vehicles": ["Vehicle1"],
            "_id_media_types": ["1"],
            "name_media_types": ["Type1"],
        }
    )

    pd.testing.assert_frame_equal(result_df, expected_result)


@pytest.mark.functions
@patch("viadot.sources.mediatool.handle_api_response")
def test_to_df_organization_ids_empty_list(mock_handle_api_response):
    """Test Mediatool `to_df` method when `organization_ids` is empty list."""
    mediatool = Mediatool(credentials=variables["credentials"])
    mock_organizations_response = MagicMock()
    mock_organizations_response.text = json.dumps(variables["organizations"])
    mock_handle_api_response.return_value = mock_organizations_response

    with pytest.raises(
        ValueError, match="'organization_ids' must be a non-empty list."
    ):
        mediatool.to_df(organization_ids=[])


@pytest.mark.functions
@patch("viadot.sources.mediatool.handle_api_response")
def test_to_df_invalid_organization_id(mock_handle_api_response):
    """Test Mediatool `to_df` method with an invalid `organization_id`."""
    mock_organizations_response = MagicMock()
    mock_organizations_response.text = json.dumps(variables["organizations"])
    mediatool = Mediatool(credentials=variables["credentials"])
    mock_handle_api_response.return_value = mock_organizations_response

    with pytest.raises(
        ValueError, match="Organization - 2 not found in organizations list."
    ):
        mediatool.to_df(organization_ids=["2"])
