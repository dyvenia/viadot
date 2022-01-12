from viadot.tasks import C4CToDF, C4CReportToDF
from viadot.config import local_config


def test_c4c_to_df():
    url = "http://services.odata.org/V2/Northwind/Northwind.svc/"
    endpoint = "Employees"
    c4c_to_df = C4CToDF()
    df = c4c_to_df.run(url=url, endpoint=endpoint)
    answer = df.head()

    assert answer.shape[1] == 23


def test_c4c_report_to_df():
    credentials = local_config.get("CLOUD_FOR_CUSTOMERS")
    credentials_prod = credentials["Prod"]
    report_url = credentials_prod["server"]
    c4c_report_to_df = C4CReportToDF()
    df = c4c_report_to_df.run(report_url=report_url, env="Prod")
    answer = df.head()

    assert answer.shape[0] == 5
