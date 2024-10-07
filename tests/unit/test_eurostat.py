"""'test_eurostat.py'."""

from unittest.mock import patch

import pytest

from viadot.sources import Eurostat


@pytest.fixture
def eurostat_instance():
    return Eurostat(dataset_code="TEIBS020", params={"unit": "EUR"})


def test_to_df():
    mock_response_data = {
        "id": ["geo", "time"],
        "dimension": {
            "geo": {
                "category": {
                    "index": {"EU": 0, "US": 1},
                    "label": {"EU": "European Union", "US": "United States"},
                }
            },
            "time": {
                "category": {
                    "index": {"2020": 0, "2021": 1},
                    "label": {"2020": "Year 2020", "2021": "Year 2021"},
                }
            },
        },
        "value": [100] * 850,  # Simulating enough values for 850 rows
        "label": "Mean and median income by household type - EU-SILC and ECHP surveys",
        "updated": "2024-10-01T23:00:00+0200",
    }

    with patch("viadot.utils.handle_api_response") as mock_handle_api_response:
        mock_handle_api_response.return_value.json.return_value = mock_response_data

        eurostat = Eurostat(
            dataset_code="ILC_DI04",
            params={"hhtyp": "total", "indic_il": "med_e"},
            columns=["geo", "time"],
        )

        df = eurostat.to_df()

        assert df.shape == (850, 7)
        assert all(
            col in df.columns
            for col in ["geo", "time", "indicator", "label", "updated"]
        )
        assert df["label"].iloc[0] == mock_response_data["label"]


def test_validate_params_invalid_key(mocker, eurostat_instance):
    mocker.patch.object(
        eurostat_instance,
        "get_parameters_codes",
        return_value={"unit": ["EUR"], "geo": ["EU"]},
    )

    params = {"unit": "EUR", "country": "US"}
    with pytest.raises(ValueError, match="Wrong parameters or codes were provided!"):
        eurostat_instance.validate_params("TEIBS020", "", params)


@patch("viadot.utils.handle_api_response")
def test_validate_params_invalid_value(mock_handle_api_response, eurostat_instance):
    mock_handle_api_response.return_value.json.return_value = {
        "id": ["geo"],
        "dimension": {
            "geo": {"category": {"index": {"EU": 0}, "label": {"EU": "European Union"}}}
        },
    }

    with pytest.raises(ValueError, match="Wrong parameters or codes were provided"):
        eurostat_instance.validate_params(
            dataset_code="TEIBS020",
            url="https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data/ILC_DI04?format=JSON&lang=EN",
            params={"geo": "US"},
        )


def test_to_df_invalid_params_type(eurostat_instance):
    eurostat_instance.params = "invalid_type"

    with pytest.raises(TypeError, match="Params should be a dictionary."):
        eurostat_instance.to_df()
