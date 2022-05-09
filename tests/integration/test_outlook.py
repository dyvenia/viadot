from viadot.sources import Outlook


def test_outlook_to_df():
    outlook = Outlook(
        mailbox_name="bulgaria@velux.com",
        start_date="2022-04-28",
        end_date="2022-04-29",
    )
    df = outlook.to_df()
    assert df.shape[1] == 10
    assert df.empty == False
