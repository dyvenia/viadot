from viadot.config import local_config
from viadot.tasks import EpicorOrdersToDF


def test_epicor_orders_to_df():
    task = EpicorOrdersToDF(
        config_key="EPICOR",
        base_url=local_config.get("EPICOR").get("test_url"),
        filters_xml="""
<OrderQuery>
    <QueryFields>
        <CompanyNumber>001</CompanyNumber>
        <BegInvoiceDate>2022-05-16</BegInvoiceDate>
        <EndInvoiceDate>2022-05-16</EndInvoiceDate>
        <RecordCount>3</RecordCount>
    </QueryFields>
</OrderQuery>""",
    )
    df = task.run()
    assert df.shape == (24, 60)
