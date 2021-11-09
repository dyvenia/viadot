import logging
import os

import pytest
from prefect.storage import Local
from viadot.flows import CloudForCustomersReportToADLS
from viadot.config import local_config


def test_cloud_for_customers_report_to_adls():
    credentials = local_config.get("CLOUD_FOR_CUSTOMERS")
    channels = ["VEL_B_AFS", "VEL_B_ASA"]
    m = ["01"]
    y = ["2021"]
    flow = CloudForCustomersReportToADLS(
        direct_url=credentials["server_QA_testing"],
        channels=channels,
        months=m,
        years=y,
        name="test_c4c_report_to_adls",
        local_file_path=f"test_c4c_report_to_adls.csv",
        adls_sp_credentials_secret=credentials["adls_sp_credentials_secret"],
        adls_dir_path=credentials["adls_dir_path"],
    )
    multiplication = len(m) * len(y) * len(channels)
    assert len(flow.urls_for_month) == multiplication

    result = flow.run()
    assert result.is_successful()

    task_results = result.result.values()
    assert all([task_result.is_successful() for task_result in task_results])
