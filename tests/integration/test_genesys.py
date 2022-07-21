import pytest
import pandas as pd
from typing import List
from viadot.sources import Genesys


@pytest.mark.init
def test_create_genesys_class():
    g = Genesys()
    assert g


@pytest.mark.init
def test_default_credential_param():
    g = Genesys()
    assert g.credentials != None and type(g.credentials) == dict


@pytest.mark.init
def test_environment_param():
    g = Genesys()
    assert g.environment != None and type(g.environment) == str


@pytest.mark.init
def test_schedule_id_param():
    g = Genesys()
    assert g.schedule_id != None and type(g.schedule_id) == str


@pytest.mark.init
def test_report_url_param():
    g = Genesys()
    assert g.report_url != None and type(g.report_url) == str


@pytest.mark.init
def test_report_clomuns_param():
    g = Genesys()
    assert g.report_columns != None and type(g.report_columns) == List


@pytest.mark.parametrize("input_name", ["", "test_name", "12345", ".##@@"])
@pytest.mark.init
def test_other_inicial_params(input_name):
    g = Genesys(report_name=input_name)
    assert len(g.report_name) > 0 and type(g.report_name) == str
