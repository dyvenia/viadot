from viadot.sources import Launchpad


def test_to_df(launchpad_url, launchpad_config_key):
    lp = Launchpad(url=launchpad_url, config_key=launchpad_config_key)

    df = lp.to_df()

    assert not df.empty
