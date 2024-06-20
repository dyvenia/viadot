import logging

import pandas as pd
import pytest
from viadot.sources import Eurostat


def test_and_validate_dataset_code_without_params(caplog):
    """Tests that the data retrieval feature returns a non-empty DataFrame for a valid dataset code."""
    task = Eurostat(dataset_code="ILC_DI04").to_df()

    assert isinstance(task, pd.DataFrame)
    assert not task.empty
    assert caplog.text == ""


def test_wrong_dataset_code_logger(caplog):
    """Tests that the error logging feature correctly identifies and logs errors for incorrect dataset codes."""

    task = Eurostat(dataset_code="ILC_DI04E")

    with pytest.raises(ValueError, match="DataFrame is empty!"):
        with caplog.at_level(logging.ERROR):
            task.to_df()
    assert (
        "Failed to fetch data for ILC_DI04E, please check correctness of dataset code!"
        in caplog.text
    )


def test_wrong_parameters_codes_logger(caplog):
    """Tests error logging for incorrect parameter codes with a correct dataset code."""

    with pytest.raises(ValueError, match="DataFrame is empty!"):
        with caplog.at_level(logging.ERROR):
            Eurostat(
                dataset_code="ILC_DI04",
                params={"hhtyp": "total1", "indic_il": "non_existing_code"},
            ).to_df()
    assert (
        "Parameters codes: 'total1 | non_existing_code' are not available. Please check your spelling!"
        in caplog.text
    )
    assert (
        "You can find everything via link: https://ec.europa.eu/eurostat/databrowser/view/ILC_DI04/default/table?lang=en"
        in caplog.text
    )


def test_parameter_codes_as_list_logger(caplog):
    """Tests error logging for incorrect parameter codes structure with a correct dataset code."""

    with pytest.raises(ValueError, match="Wrong structure of params!"):
        with caplog.at_level(logging.ERROR):
            Eurostat(
                dataset_code="ILC_DI04",
                params={"hhtyp": ["totale", "nottotale"], "indic_il": "med_e"},
            ).to_df()

    assert (
        "You can provide only one code per one parameter as 'str' in params!\n"
        in caplog.text
    )
    assert (
        "CORRECT: params = {'unit': 'EUR'} | INCORRECT: params = {'unit': ['EUR', 'USD', 'PLN']}"
        in caplog.text
    )


def test_wrong_parameters(caplog):
    """Tests error logging for incorrect parameter keys with a correct dataset code."""

    task = Eurostat(
        dataset_code="ILC_DI04",
        params={"hhhtyp": "total", "indic_ilx": "med_e"},
    )

    with pytest.raises(ValueError, match="DataFrame is empty!"):
        with caplog.at_level(logging.ERROR):
            task.to_df()
    assert (
        "Parameters: 'hhhtyp | indic_ilx' are not in dataset. Please check your spelling!\n"
        in caplog.text
    )
    assert (
        "Possible parameters: freq | hhtyp | indic_il | unit | geo | time"
        in caplog.text
    )


def test_params_as_list():
    """Tests error logging for incorrect parameter structure with a correct dataset code."""

    with pytest.raises(TypeError, match="Params should be a dictionary."):
        Eurostat(dataset_code="ILC_DI04", params=["total", "med_e"]).to_df()


def test_correct_params_and_dataset_code(caplog):
    """Tests that the data retrieval feature returns a non-empty DataFrame for a valid dataset code with correct parameters."""

    task = Eurostat(
        dataset_code="ILC_DI04", params={"hhtyp": "total", "indic_il": "med_e"}
    ).to_df()

    assert isinstance(task, pd.DataFrame)
    assert not task.empty
    assert caplog.text == ""


def task_correct_requested_columns(caplog):
    """Tests error logging for correct dataset code, parameters, and requested columns."""

    task = Eurostat(
        dataset_code="ILC_DI04",
        params={"hhtyp": "total", "indic_il": "med_e"},
        requested_columns=["updated", "geo", "indicator"],
    ).to_df()

    assert isinstance(task, pd.DataFrame)
    assert not task.empty
    assert caplog.text == ""
    assert list(task.columns) == task.requested_columns


def test_wrong_needed_columns_names(caplog):
    """Tests error logging for incorrect names of requested columns with a correct dataset code and parameters."""

    task = Eurostat(
        dataset_code="ILC_DI04",
        params={"hhtyp": "total", "indic_il": "med_e"},
        requested_columns=["updated1", "geo1", "indicator1"],
    )

    with pytest.raises(ValueError, match="Provided columns are not available!"):
        with caplog.at_level(logging.ERROR):
            task.to_df()
    assert (
        "Name of the columns: 'updated1 | geo1 | indicator1' are not in DataFrame. Please check spelling!\n"
        in caplog.text
    )
    assert "Available columns: geo | time | indicator | label | updated" in caplog.text


def test_wrong_params_and_wrong_requested_columns_names(caplog):
    """Tests error logging for incorrect parameters and names of requested columns with a correct dataset code."""

    task = Eurostat(
        dataset_code="ILC_DI04",
        params={"hhhtyp": "total", "indic_ilx": "med_e"},
        requested_columns=["updated1", "geo1", "indicator1"],
    )

    with pytest.raises(ValueError, match="DataFrame is empty!"):
        with caplog.at_level(logging.ERROR):
            task.to_df()
    assert (
        "Parameters: 'hhhtyp | indic_ilx' are not in dataset. Please check your spelling!\n"
        in caplog.text
    )
    assert (
        "Possible parameters: freq | hhtyp | indic_il | unit | geo | time"
        in caplog.text
    )


def test_requested_columns_not_in_list():
    """Tests error logging for incorrect requested columns structure with a correct dataset code and parameters."""

    task = Eurostat(
        dataset_code="ILC_DI04",
        params={"hhtyp": "total", "indic_il": "med_e"},
        requested_columns="updated",
    )
    with pytest.raises(
        TypeError, match="Requested columns should be provided as list of strings."
    ):
        task.to_df()
