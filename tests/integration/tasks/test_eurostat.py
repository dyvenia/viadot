import logging

import pandas as pd
import pytest

from viadot.tasks import EurostatToDF


def task_correct_requested_columns(caplog):
    """This function is designed to test the accuracy of the data retrieval feature in a program.
    Specifically, it tests to ensure that the program is able to correctly identify and log error
    when provided with a correct dataset_code, correct params and correct requested_columns.
    The function is intended to be used in software development to verify that the program is correctly
    retrieving data from the appropriate dataset.
    """
    task = EurostatToDF(
        dataset_code="ILC_DI04",
        params={"hhtyp": "total", "indic_il": "med_e"},
        requested_columns=["updated", "geo", "indicator"],
    )
    task.run()

    assert isinstance(task, pd.DataFrame)
    assert not task.empty
    assert caplog.text == ""
    assert list(task.columns) == task.requested_columns


def test_wrong_needed_columns_names(caplog):
    """This function is designed to test the accuracy of the error logging feature in a program.
    Specifically, it tests to ensure that the program is able to correctly identify and log error
    when provided with a correct dataset_code, correct parameters, but incorrect names of requested columns.
    The function is intended to be used in software development to identify correct type errors
    and messages in the program's handling of codes.
    """
    task = EurostatToDF(
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


def test_wrong_params_and_wrong_requested_columns_names(caplog):
    """This function is designed to test the accuracy of the error logging feature in a program.
    Specifically, it tests to ensure that the program is able to correctly identify and log error
    when provided with a correct dataset_code, incorrect parameters and incorrect names of requested columns.
    Test should log errors only related with wrong params - we are trying to check if program will stop after
    params validation. The function is intended to be used in software development to identify correct type errors
    and messages in the program's handling of codes.
    """
    task = EurostatToDF(
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
    """This function is designed to test the accuracy of the error logging feature in a program.
    Specifically, it tests to ensure that the program is able to correctly identify and log error
    when provided with a correct dataset_code, correct params but incorrect requested_columns structure
    (as single string instead of list with strings).
    The function is intended to be used in software development to identify correct type errors
    and messages in the program's handling of codes.
    """
    with pytest.raises(
        TypeError, match="Requested columns should be provided as list of strings."
    ):
        EurostatToDF(
            dataset_code="ILC_DI04",
            params={"hhtyp": "total", "indic_il": "med_e"},
            requested_columns="updated",
        ).run()


def test_requested_columns_not_provided(caplog):
    """Test the behavior when 'requested_columns' are not provided to EurostatToDF.

    This test checks the behavior of the EurostatToDF class when 'requested_columns' are not provided.
    It ensures that the resulting DataFrame is of the correct type, not empty, and that no error
    messages are logged using the 'caplog' fixture.

    Parameters:
    - caplog: pytest fixture for capturing log messages.

    Usage:
    - Invoke this test function to check the behavior of EurostatToDF when 'requested_columns' are not provided.
    """
    task = EurostatToDF(
        dataset_code="ILC_DI04",
        params={"hhtyp": "total", "indic_il": "med_e"},
    )
    df = task.run()

    assert isinstance(df, pd.DataFrame)
    assert not df.empty
    assert caplog.text == ""
