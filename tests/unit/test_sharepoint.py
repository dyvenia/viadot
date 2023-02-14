import os

from viadot.sources import Sharepoint


def test_to_df(sharepoint_url):
    s = Sharepoint(config_key="sharepoint_prod")
    df = s.to_df(sharepoint_url)
    assert not df.empty


def test_download_file(sharepoint_url):
    s = Sharepoint(config_key="sharepoint_prod")

    file = "sharepoint_test" + sharepoint_url.split(".")[-1]
    s.download_file(url=sharepoint_url, to_path=file)

    assert file in os.listdir()

    os.remove(file)
