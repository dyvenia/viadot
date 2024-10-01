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
    "media_entries": {"mediaEntries": [{"_id": "1", "name": "Entry1"}]},
    "vehicle": {"vehicle": {"_id": "1", "name": "Vehicle1"}},
    "campaigns": {"campaigns": [{"_id": "1", "name": "Campaign1"}]},
    "media_types": {"mediaType": {"_id": "1", "name": "Type1", "type": "Type"}},
}


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
    expected_result = [{"_id": "1", "name": "Entry1"}]
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
    expected_result = [{"_id": "1", "name": "Campaign1"}]
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


@pytest.mark.functions
def test_rename_columns():
    """Test Mediatool `_rename_columns` function."""
    df = pd.DataFrame({"col1": [1, 2], "col2": [3, 4]})

    mediatool = Mediatool(credentials=variables["credentials"])

    result = mediatool._rename_columns(df, column_suffix="test")
    expected_result = pd.DataFrame({"col1_test": [1, 2], "col2_test": [3, 4]})
    pd.testing.assert_frame_equal(result, expected_result)


@pytest.mark.connect
@patch("viadot.sources.mediatool.handle_api_response")
def test_api_connection(mock_handle_api_response):
    """Test Mediatool `api_connection` method."""
    mock_response = MagicMock()
    mock_response.text = json.dumps(variables["organizations"])
    mock_handle_api_response.return_value = mock_response

    mediatool = Mediatool(credentials=variables["credentials"])

    result = mediatool.api_connection(get_data_from="organizations")
    expected_result = [{"_id": "1", "name": "Org1", "abbreviation": "O1"}]
    assert result == expected_result


@pytest.mark.functions
@patch("viadot.sources.mediatool.handle_api_response")
def test_to_df(mock_handle_api_response):
    """Test Mediatool `to_df` method."""
    mock_response = MagicMock()
    mock_response.text = json.dumps({"mediaEntries": [{"_id": "1", "name": "Entry1"}]})
    mock_handle_api_response.return_value = mock_response

    mediatool = Mediatool(credentials=variables["credentials"])

    data = [{"_id": "1", "name": "Entry1"}]
    result_df = mediatool.to_df(data=data, column_suffix="media_entries")
    result_df.drop(
        columns=["_viadot_source", "_viadot_downloaded_at_utc"],
        inplace=True,
        axis=1,
    )
    expected_result = pd.DataFrame(
        {"_id_media_entries": ["1"], "name_media_entries": ["Entry1"]}
    )
    pd.testing.assert_frame_equal(result_df, expected_result)
