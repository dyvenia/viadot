import logging

import pytest
from viadot.sources import Eurostat


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
