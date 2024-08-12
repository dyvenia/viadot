import pandas as pd
import pytest
from viadot.config import get_source_credentials
from viadot.exceptions import DataRangeError
from viadot.sources import Epicor

print(get_source_credentials("epicor").get("test_url"))


@pytest.fixture(scope="session")
def epicor():
    epicor = Epicor(
        base_url=get_source_credentials("epicor").get("test_url"),
        config_key="epicor",
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
    yield epicor


@pytest.fixture(scope="session")
def epicor_error():
    epicor_error = Epicor(
        base_url=get_source_credentials("epicor").get("test_url"),
        config_key="epicor",
        filters_xml="""
    <OrderQuery>
        <QueryFields>
            <CompanyNumber>001</CompanyNumber>
            <BegInvoiceDate></BegInvoiceDate>
            <EndInvoiceDate>2022-05-16</EndInvoiceDate>
            <RecordCount>3</RecordCount>
        </QueryFields>
    </OrderQuery>""",
    )
    yield epicor_error


def test_connection(epicor):
    assert epicor.get_xml_response().ok


def test_validate_filter(epicor_error):
    with pytest.raises(DataRangeError):
        epicor_error.validate_filter()


def test_to_df_return_type(epicor):
    df = epicor.to_df()
    assert isinstance(df, pd.DataFrame)
