from viadot.tasks import c4c_to_df, c4c_report_to_df
from viadot.config import local_config


def test_c4c_to_df():
    credentials = local_config.get("CLOUD_FOR_CUSTOMERS")
    credentials_prod = credentials["Prod"]
    report_url = credentials_prod["server"]
    df = c4c_to_df.run(report_url=report_url, env="Prod")
    answer = df.head()
    assert answer.shape[1] == 28


def test_c4c_report_to_df():
    credentials = local_config.get("CLOUD_FOR_CUSTOMERS")
    credentials_prod = credentials["Prod"]
    report_url = credentials_prod["server"]
    df = c4c_report_to_df.run(report_url=report_url, env="Prod")
    answer = df.head()
    assert answer.shape[0] == 5
