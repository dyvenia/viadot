import pytest
import pandas as pd
import logging

from viadot.tasks import eurostat


def test_correct_dataset_code_no_params(caplog):
    """Test for checking if program for correct dataset_code
    and no parameters is returning non empty DataFrame
    """
    task = eurostat.EurostatToDF(dataset_code="ILC_DI04").run()
    assert isinstance(task, pd.DataFrame)
    assert not task.empty
    assert caplog.text == ""


def test_wrong_dataset_code_logger(caplog):
    """Test for checking if program for incorrect dataset_code
    is logging correct error
    """
    task = eurostat.EurostatToDF(dataset_code="ILC_DI04E")

    with pytest.raises(ValueError, match="DataFrame is empty!"):
        with caplog.at_level(logging.ERROR):
            task.run()
    assert (
        f"Failed to fetch data for ILC_DI04E, please check correctness of dataset code!"
        in caplog.text
    )


def test_wrong_parameters_codes_logger(caplog):
    """Test for checking if program for correct dataset_code
    and correct parameters, but incorrect parameters codes
    is logging correct error
    """
    task = eurostat.EurostatToDF(
        dataset_code="ILC_DI04",
        params={"hhtyp": "total1", "indic_il": "non_existing_code"},
    )

    with pytest.raises(ValueError, match="DataFrame is empty!"):
        with caplog.at_level(logging.ERROR):
            task.run()
    assert (
        f"Parameters codes: 'total1 | non_existing_code' are not available. Please check your spelling!"
        in caplog.text
    )
    assert (
        f"You can find everything via link: https://ec.europa.eu/eurostat/databrowser/view/ILC_DI04/default/table?lang=en"
        in caplog.text
    )


def test_parameter_codes_as_list_loggere(caplog):
    """Test for checking if program for correct dataset_code,
    correct parameters, but incorrect codes provided as list
    is logging correct error
    """

    task = eurostat.EurostatToDF(
        dataset_code="ILC_DI04",
        params={"hhtyp": ["totale", "nottotale"], "indic_il": "med_e"},
    )
    with pytest.raises(ValueError, match="Wrong structure of params!"):
        with caplog.at_level(logging.ERROR):
            task.run()
    assert (
        "You can provide only one code per one parameter as 'str' in params!\n"
        in caplog.text
    )
    assert (
        "CORRECT: params = {'unit': 'EUR'} | INCORRECT: params = {'unit': ['EUR', 'USD', 'PLN']}"
        in caplog.text
    )


def test_wrong_parameters(caplog):
    """Test for checking if program for correct dataset_code,
    incorrect parameters and correct codes
    is logging correct error
    """
    task = eurostat.EurostatToDF(
        dataset_code="ILC_DI04", params={"hhhtyp": "total", "indic_ilx": "med_e"}
    )
    with pytest.raises(ValueError, match="DataFrame is empty!"):
        with caplog.at_level(logging.ERROR):
            task.run()
    assert (
        f"Parameters: 'hhhtyp | indic_ilx' are not in dataset. Please check your spelling!\n"
        in caplog.text
    )
    assert (
        f"Possible parameters: freq | hhtyp | indic_il | unit | geo | time"
        in caplog.text
    )


def test_params_as_list():
    """Test for checking if program for correct dataset_code,
    incorrect parameters structure (as list, not dict)
    is logging correct error
    """
    with pytest.raises(TypeError, match="Params should be a dictionary."):
        eurostat.EurostatToDF(dataset_code="ILC_DI04", params=["total", "med_e"]).run()


def test_correct_params_and_dataset_code(caplog):
    """Test for checking if program for correct dataset_code
    and correct parameters and correct codes
    is returning non empty DataFrame or logging error
    """
    task = eurostat.EurostatToDF(
        dataset_code="ILC_DI04", params={"hhtyp": "total", "indic_il": "med_e"}
    ).run()

    assert isinstance(task, pd.DataFrame)
    assert not task.empty
    assert caplog.text == ""


def task_correct_needed_columns(caplog):
    """Test for checking if program for correct dataset_code,
    correct parameters, correct codes
    and correct names of needed columns
    is returning none empty DataFrame
    with expected columns
    """
    task = eurostat.EurostatToDF(
        dataset_code="ILC_DI04",
        params={"hhtyp": "total", "indic_il": "med_e"},
        requested_columns=["updated", "geo", "indicator"],
    )
    task.run()

    assert isinstance(task, pd.DataFrame)
    assert not task.empty
    assert caplog.text == ""
    assert list(task.columns) == task.needed_columns


def test_wrong_needed_columns_names(caplog):
    """Test for checking if program for correct dataset_code,
    correct parameters, correct codes
    but incorrect names of needed columns
    is logging correct error
    """
    task = eurostat.EurostatToDF(
        dataset_code="ILC_DI04",
        params={"hhtyp": "total", "indic_il": "med_e"},
        requested_columns=["updated1", "geo1", "indicator1"],
    )
    with pytest.raises(ValueError, match="Provided columns are not available!"):
        with caplog.at_level(logging.ERROR):
            task.run()
    assert (
        f"Name of the columns: 'updated1 | geo1 | indicator1' are not in DataFrame. Please check spelling!\n"
        in caplog.text
    )
    assert f"Available columns: geo | time | indicator | label | updated" in caplog.text


def test_wrong_params_and_wrong_needed_columns_names(caplog):
    """Test for checking if program for correct dataset_code,
    incorrect parameters, correct codes
    and incorrect names of needed columns
    is logging correct error
    """
    task = eurostat.EurostatToDF(
        dataset_code="ILC_DI04",
        params={"hhhtyp": "total", "indic_ilx": "med_e"},
        requested_columns=["updated1", "geo1", "indicator1"],
    )
    with pytest.raises(ValueError, match="DataFrame is empty!"):
        with caplog.at_level(logging.ERROR):
            task.run()
    assert (
        f"Parameters: 'hhhtyp | indic_ilx' are not in dataset. Please check your spelling!\n"
        in caplog.text
    )
    assert (
        f"Possible parameters: freq | hhtyp | indic_il | unit | geo | time"
        in caplog.text
    )


def test_requested_columns_not_in_list():
    """Test for checking if program for correct dataset_code,
    correct parameters, correct codes
    and requested_columns as single string (not in list)
    is logging correct error
    """
    with pytest.raises(
        TypeError, match="Requested columns should be provided as list of strings."
    ):
        eurostat.EurostatToDF(
            dataset_code="ILC_DI04",
            params={"hhtyp": "total", "indic_il": "med_e"},
            requested_columns="updated",
        ).run()
