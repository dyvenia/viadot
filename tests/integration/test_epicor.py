import pandas as pd
import pytest
from viadot.config import get_source_credentials
from viadot.exceptions import DataRangeError
from viadot.sources import Epicor


@pytest.fixture(scope="session")
def epicor():
    return Epicor(
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


@pytest.fixture(scope="session")
def epicor_error():
    return Epicor(
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


def test_connection(epicor: Epicor):
    assert epicor.get_xml_response().ok


def test_validate_filter(epicor_error: Epicor):
    with pytest.raises(DataRangeError):
        epicor_error.validate_filter()


def test_to_df_return_type(epicor: Epicor):
    df = epicor.to_df()
    assert isinstance(df, pd.DataFrame)
