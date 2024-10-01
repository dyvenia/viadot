"""'test_eurostat.py'."""

import logging

import pandas as pd
import pytest

from src.viadot.sources import Eurostat


URL = (
    "https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0"
    "/data/ILC_DI04?format=JSON&lang=EN"
)


class EurostatMock(Eurostat):
    """Mock of Eurostat source class."""

    def __init__(self, dataset_code=None, params=None, columns=None):
        """Init method."""
        super().__init__(dataset_code=dataset_code, params=params, columns=columns)


def test_eurostat_dictionary_to_df():
    """Test the 'eurostat_dictionary_to_df' method for correct DataFrame creation."""
    obj = Eurostat(dataset_code="")

    # Mock data for the Eurostat API response
    mock_eurostat_data = {
        "dimension": {
            "geo": {
                "category": {
                    "index": {"0": 0, "1": 1},
                    "label": {"0": "Belgium", "1": "Germany"},
                }
            },
            "time": {
                "category": {
                    "index": {"0": 0, "1": 1},
                    "label": {"0": "2021", "1": "2022"},
                }
            },
        },
        "value": {
            "0": 100,  # Belgium, 2021
            "1": 150,  # Germany, 2021
            "2": 110,  # Belgium, 2022
            "3": 140,  # Germany, 2022
        },
    }

    signals = [["geo", "time"], mock_eurostat_data]

    df = obj.eurostat_dictionary_to_df(*signals)

    expected_columns = ["geo", "time", "indicator"]

    assert list(df.columns) == expected_columns

    expected_data = {
        "geo": ["Belgium", "Germany", "Belgium", "Germany"],
        "time": ["2021", "2021", "2022", "2022"],
        "indicator": [100, 150, 110, 140],
    }

    expected_df = pd.DataFrame(expected_data)

    pd.testing.assert_frame_equal(df, expected_df)


def test_wrong_dataset_code_logger(caplog):
    """Tests that the error logging feature correctly logs errors.

    For incorrect dataset codes.
    """
    eurostat = EurostatMock(dataset_code="ILC_DI04E")

    with pytest.raises(ValueError, match="DataFrame is empty!"):  # noqa: SIM117
        with caplog.at_level(logging.ERROR):
            eurostat.to_df()
    assert (
        "Failed to fetch data for ILC_DI04E, please check correctness of dataset code!"
        in caplog.text
    )


def test_and_validate_dataset_code_without_params(caplog):
    """Tests that the data retrieval feature returns a non-empty DataFrame."""
    eurostat = EurostatMock(dataset_code="ILC_DI04")
    df = eurostat.to_df()

    assert isinstance(df, pd.DataFrame)
    assert not df.empty
    assert caplog.text == ""


def test_wrong_parameters_codes_logger(caplog):
    """Tests error logging for incorrect parameter codes with a correct dataset code."""
    params = {"hhtyp": "total1", "indic_il": "non_existing_code"}
    eurostat = EurostatMock(dataset_code="ILC_DI04", params=params)

    with pytest.raises(ValueError, match="Wrong parameters or codes were provided!"):  # noqa: SIM117
        with caplog.at_level(logging.ERROR):
            eurostat.validate_params(
                dataset_code=eurostat.dataset_code, url=URL, params=params
            )
    assert (
        "Parameters codes: 'total1 | non_existing_code' are not available. "
        "Please check your spelling!" in caplog.text
    )


def test_parameter_codes_as_list_logger(caplog):
    """Tests error logging for incorrect parameter codes structure."""
    params = {"hhtyp": ["totale", "nottotale"], "indic_il": "med_e"}
    eurostat = EurostatMock(dataset_code="ILC_DI04", params=params)

    with pytest.raises(TypeError, match="Wrong structure of params!"):  # noqa: SIM117
        with caplog.at_level(logging.ERROR):
            eurostat.validate_params(
                dataset_code=eurostat.dataset_code, url=URL, params=params
            )
    assert (
        "You can provide only one code per one parameter as 'str' in params! "
        "CORRECT: params = {'unit': 'EUR'} | INCORRECT: params = "
        "{'unit': ['EUR', 'USD', 'PLN']}" in caplog.text
    )


def test_wrong_parameters(caplog):
    """Tests error logging for incorrect parameter keys."""
    params = {"hhhtyp": "total", "indic_ilx": "med_e"}
    eurostat = EurostatMock(dataset_code="ILC_DI04", params=params)

    with pytest.raises(ValueError, match="Wrong parameters or codes were provided!"):  # noqa: SIM117
        with caplog.at_level(logging.ERROR):
            eurostat.validate_params(
                dataset_code=eurostat.dataset_code, url=URL, params=params
            )
    assert (
        "Parameters: 'hhhtyp | indic_ilx' are not in dataset. "
        "Please check your spelling!" in caplog.text
    )


def test_correct_params_and_dataset_code(caplog):
    """Tests that the data retrieval feature returns a non-empty DataFrame.

    For valid dataset code.
    """
    eurostat = EurostatMock(
        dataset_code="ILC_DI04", params={"hhtyp": "total", "indic_il": "med_e"}
    )
    df = eurostat.to_df()

    assert isinstance(df, pd.DataFrame)
    assert not df.empty
    assert caplog.text == ""


def test_wrong_needed_columns_names(caplog):
    """Tests error logging for incorrect names of requested columns."""
    eurostat = EurostatMock(
        dataset_code="ILC_DI04",
        params={"hhtyp": "total", "indic_il": "med_e"},
        columns=["updated1", "geo1", "indicator1"],
    )

    with pytest.raises(ValueError, match="Provided columns are not available!"):  # noqa: SIM117
        with caplog.at_level(logging.ERROR):
            eurostat.to_df()
    assert (
        "Name of the columns: 'updated1 | geo1 | indicator1' are not in DataFrame. "
        "Please check spelling!" in caplog.text
    )


def test_wrong_params_and_wrong_requested_columns_names(caplog):
    """Tests error logging for incorrect parameters and names of requested columns."""
    eurostat = EurostatMock(
        dataset_code="ILC_DI04",
        params={"hhhtyp": "total", "indic_ilx": "med_e"},
        columns=["updated1", "geo1", "indicator1"],
    )

    with pytest.raises(ValueError, match="Wrong parameters or codes were provided!"):  # noqa: SIM117
        with caplog.at_level(logging.ERROR):
            eurostat.to_df()
    assert (
        "Parameters: 'hhhtyp | indic_ilx' are not in dataset. "
        "Please check your spelling!" in caplog.text
    )


def test_requested_columns_not_in_list():
    """Tests error logging for incorrect requested columns structure."""
    with pytest.raises(
        TypeError, match="Requested columns should be provided as list of strings."
    ):
        EurostatMock(
            dataset_code="ILC_DI04",
            params={"hhtyp": "total", "indic_il": "med_e"},
            columns="updated",
        ).to_df()


def test_params_as_list():
    """Tests error logging for incorrect parameter structure."""
    with pytest.raises(TypeError, match="Params should be a dictionary."):
        EurostatMock(dataset_code="ILC_DI04", params=["total", "med_e"]).to_df()
