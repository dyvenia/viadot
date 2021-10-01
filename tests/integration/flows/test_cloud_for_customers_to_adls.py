import logging
import os

import pytest
from prefect.storage import Local
from viadot.flows import CloudForCustomersToADLS
from viadot.config import local_config


def test_cloud_for_customers_to_adls():
    credentials = local_config.get("CLOUD_FOR_CUSTOMERS")
    flow = CloudForCustomersToADLS(
        "Cloud For Customers Load extract test",
        endpoint="ServiceRequestCollection",
        name="test_c4c_adls",
        url="http://services.odata.org/V2/Northwind/Northwind.svc/",
        endpoint="Employees",
        adls_sp_credentials_secret=credentials["adls_sp_credentials_secret"],
        adls_dir_path=credentials["adls_dir_path"],
    )
    result = flow.run()
    assert result.is_successful()
