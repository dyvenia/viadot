import os
from unittest import mock

import pandas as pd
import pytest

from viadot.exceptions import ValidationError
from viadot.flows import CustomerGaugeToADLS

DATA = {
    "user_name": ["Jane", "Bob"],
    "user_address_street": ["456 Elm St", "55 Oat St"],
    "user_address_city": ["San Francisco", "New York City"],
    "user_address_state": ["CA", "NY"],
    "user_address_zip": ["94109", "85674"],
    "user_address_country_name": "United States",
    "user_address_country_code": "US",
}

COLUMNS = ["user_name", "user_address_street"]
ADLS_FILE_NAME = "test_customer_gauge.parquet"
ADLS_DIR_PATH = "raw/tests/"


@mock.patch(
    "viadot.tasks.CustomerGaugeToDF.run",
    return_value=pd.DataFrame(data=DATA),
)
@pytest.mark.run
def test_customer_gauge_to_adls_run_flow(mocked_class):
    flow = CustomerGaugeToADLS(
        "test_customer_gauge_to_adls_flow_run",
        endpoint="responses",
        total_load=False,
        anonymize=True,
        columns_to_anonymize=COLUMNS,
        adls_dir_path=ADLS_DIR_PATH,
        adls_file_name=ADLS_FILE_NAME,
        overwrite_adls=True,
    )
    result = flow.run()
    assert result.is_successful()
    os.remove("test_customer_gauge_to_adls_flow_run.parquet")
    os.remove("test_customer_gauge_to_adls_flow_run.json")


@mock.patch(
    "viadot.tasks.CustomerGaugeToDF.run",
    return_value=pd.DataFrame(data=DATA),
)
@pytest.mark.run
def test_customer_gauge_to_adls_run_flow_validation_success(mocked_class):
    flow = CustomerGaugeToADLS(
        "test_customer_gauge_to_adls_run_flow_validation_success",
        endpoint="responses",
        total_load=False,
        anonymize=True,
        columns_to_anonymize=COLUMNS,
        adls_dir_path=ADLS_DIR_PATH,
        adls_file_name=ADLS_FILE_NAME,
        overwrite_adls=True,
        validate_df_dict={"column_size": {"user_address_state": 2}},
    )
    result = flow.run()
    assert result.is_successful()
    assert len(flow.tasks) == 11

    os.remove("test_customer_gauge_to_adls_run_flow_validation_success.parquet")
    os.remove("test_customer_gauge_to_adls_run_flow_validation_success.json")


@mock.patch(
    "viadot.tasks.CustomerGaugeToDF.run",
    return_value=pd.DataFrame(data=DATA),
)
@pytest.mark.run
def test_customer_gauge_to_adls_run_flow_validation_failure(mocked_class):
    flow = CustomerGaugeToADLS(
        "test_customer_gauge_to_adls_run_flow_validation_failure",
        endpoint="responses",
        total_load=False,
        anonymize=True,
        columns_to_anonymize=COLUMNS,
        adls_dir_path=ADLS_DIR_PATH,
        adls_file_name=ADLS_FILE_NAME,
        overwrite_adls=True,
        validate_df_dict={"column_size": {"user_name": 5}},
    )
    try:
        flow.run()
    except ValidationError:
        pass

    os.remove("test_customer_gauge_to_adls_run_flow_validation_failure.parquet")
    os.remove("test_customer_gauge_to_adls_run_flow_validation_failure.json")
