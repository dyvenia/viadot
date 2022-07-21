from viadot.config import local_config
from viadot.flows import CloudForCustomersReportToADLS


def test_cloud_for_customers_report_to_adls():
    credentials = local_config.get("CLOUD_FOR_CUSTOMERS")
    credentials_prod = credentials["Prod"]
    channels = ["VEL_B_AFS", "VEL_B_ASA"]
    month = ["01"]
    year = ["2021"]
    flow = CloudForCustomersReportToADLS(
        report_url=credentials_prod["server"],
        env="Prod",
        channels=channels,
        months=month,
        years=year,
        name="test_c4c_report_to_adls",
        local_file_path=f"test_c4c_report_to_adls.csv",
        adls_sp_credentials_secret=credentials["adls_sp_credentials_secret"],
        adls_dir_path=credentials["adls_dir_path"],
    )
    number_of_urls = len(month) * len(year) * len(channels)
    assert len(flow.report_urls_with_filters) == number_of_urls

    result = flow.run()
    assert result.is_successful()

    task_results = result.result.values()
    assert all([task_result.is_successful() for task_result in task_results])
