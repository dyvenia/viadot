from viadot.sources import Sharepoint


def test_to_df(sharepoint_url):
    s = Sharepoint(config_key="sharepoint_prod")
    df = s.to_df(sharepoint_url)
    assert not df.empty
