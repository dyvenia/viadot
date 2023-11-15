import logging

import pandas as pd
import pytest

from viadot.sources import Eurostat


def test_and_validate_dataset_code_without_params(caplog):
    """This function is designed to test the accuracy of the data retrieval feature in a program.
    Specifically, it tests to ensure that the program returns a non-empty DataFrame when a correct
    dataset code is provided without any parameters. The function is intended to be used in software
    development to verify that the program is correctly retrieving data from the appropriate dataset.
    """
    source = Eurostat(dataset_code="ILC_DI04").get_data_frame_from_response()
    assert isinstance(source, pd.DataFrame)
    assert not source.empty
    assert caplog.text == ""


def test_wrong_dataset_code_logger(caplog):
    """This function is designed to test the accuracy of the error logging feature in a program.
    Specifically, it tests to ensure that the program is able to correctly identify and log errors
    when provided with only incorrect dataset code.
    The function is intended to be used in software development to identify correct type errors
    and messages in the program's handling of codes.
    """
    source = Eurostat(dataset_code="ILC_DI04E")

    with pytest.raises(ValueError, match="DataFrame is empty!"):
        with caplog.at_level(logging.ERROR):
            source.get_data_frame_from_response()
    assert (
        f"Failed to fetch data for ILC_DI04E, please check correctness of dataset code!"
        in caplog.text
    )


def test_wrong_parameters_codes_logger(caplog):
    """This function is designed to test the accuracy of the error logging feature in a program.
    Specifically, it tests to ensure that the program is able to correctly identify and log errors
    when provided with a correct dataset_code and correct parameters are provided, but both parameters codes are incorrect.
    The function is intended to be used in software development to identify correct type errors
    and messages in the program's handling of codes.
    """
    source = Eurostat(
        dataset_code="ILC_DI04",
        params={"hhtyp": "total1", "indic_il": "non_existing_code"},
    )

    with pytest.raises(ValueError, match="DataFrame is empty!"):
        with caplog.at_level(logging.ERROR):
            source.get_data_frame_from_response()
    assert (
        f"Parameters codes: 'total1 | non_existing_code' are not available. Please check your spelling!"
        in caplog.text
    )
    assert (
        f"You can find everything via link: https://ec.europa.eu/eurostat/databrowser/view/ILC_DI04/default/table?lang=en"
        in caplog.text
    )


def test_parameter_codes_as_list_logger(caplog):
    """This function is designed to test the accuracy of the error logging feature in a program.
    Specifically, it tests to ensure that the program is able to correctly identify and log errors
    when provided with a correct dataset code, correct parameters, but incorrect parameters codes structure
    (as a list with strings, instead of single string).
    The function is intended to be used in software development to identify correct type errors
    and messages in the program's handling of codes.
    """

    source = Eurostat(
        dataset_code="ILC_DI04",
        params={"hhtyp": ["totale", "nottotale"], "indic_il": "med_e"},
    )
    with pytest.raises(ValueError, match="Wrong structure of params!"):
        with caplog.at_level(logging.ERROR):
            source.get_data_frame_from_response()
    assert (
        "You can provide only one code per one parameter as 'str' in params!\n"
        in caplog.text
    )
    assert (
        "CORRECT: params = {'unit': 'EUR'} | INCORRECT: params = {'unit': ['EUR', 'USD', 'PLN']}"
        in caplog.text
    )


def test_wrong_parameters(caplog):
    """This function is designed to test the accuracy of the error logging feature in a program.
    Specifically, it tests to ensure that the program is able to correctly identify and log errors
    when provided with a correct dataset_code, but incorrect parameters keys.
    The function is intended to be used in software development to identify correct type errors
    and messages in the program's handling of codes.
    """

    source = Eurostat(
        dataset_code="ILC_DI04", params={"hhhtyp": "total", "indic_ilx": "med_e"}
    )
    with pytest.raises(ValueError, match="DataFrame is empty!"):
        with caplog.at_level(logging.ERROR):
            source.get_data_frame_from_response()
    assert (
        f"Parameters: 'hhhtyp | indic_ilx' are not in dataset. Please check your spelling!\n"
        in caplog.text
    )
    assert (
        f"Possible parameters: freq | hhtyp | indic_il | unit | geo | time"
        in caplog.text
    )


def test_params_as_list():
    """This function is designed to test the accuracy of the error logging feature in a program.
    Specifically, it tests to ensure that the program is able to correctly identify and log error
    when provided with a correct dataset_code, but incorrect params structure (as list instead of dict).
    The function is intended to be used in software development to identify correct type errors
    and messages in the program's handling of codes.
    """
    with pytest.raises(TypeError, match="Params should be a dictionary."):
        Eurostat(
            dataset_code="ILC_DI04", params=["total", "med_e"]
        ).get_data_frame_from_response()


def test_correct_params_and_dataset_code(caplog):
    """This function is designed to test the accuracy of the data retrieval feature in a program.
    Specifically, it tests to ensure that the program returns a non-empty DataFrame when a correct
    dataset code is provided with correct params. The function is intended to be used in software
    development to verify that the program is correctly retrieving data from the appropriate dataset.
    """

    source = Eurostat(
        dataset_code="ILC_DI04", params={"hhtyp": "total", "indic_il": "med_e"}
    ).get_data_frame_from_response()

    assert isinstance(source, pd.DataFrame)
    assert not source.empty
    assert caplog.text == ""
