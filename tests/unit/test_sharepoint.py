import os

from viadot.sources import Sharepoint


def test_to_df(sharepoint_url, sharepoint_config_key):
    s = Sharepoint(config_key=sharepoint_config_key)
    df = s.to_df(sharepoint_url)
    assert not df.empty


def test_download_file(sharepoint_url, sharepoint_config_key):
    s = Sharepoint(config_key=sharepoint_config_key)

    file = "sharepoint_test" + sharepoint_url.split(".")[-1]
    s.download_file(url=sharepoint_url, to_path=file)

    assert file in os.listdir()

    os.remove(file)
