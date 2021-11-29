from viadot.sources import CloudForCustomers


def test_credentials():
    endpoint = "ServiceRequestCollection"
    c4c = CloudForCustomers(endpoint=endpoint, params={"$top": "2"})
    df = c4c.to_df(
        fields=["ProductRecipientPartyName", "CreationDateTime", "CreatedBy"]
    )
    assert df.empty == False
