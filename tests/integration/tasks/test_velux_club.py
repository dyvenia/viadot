import pytest
import pandas as pd
from viadot.tasks.velux_club import VeluxClubToDF


@pytest.fixture(scope="session")
def var_dictionary():
    variables = {
        "source": "jobs",
        "from_date": "2022-03-23",
        # {
        #     "propertyName": "from_date",
        #     "operator": "BETWEEN",
        #     "highValue": "YYYY-MM-DD",
        #     "value": "YYYY-MM-DD",
        # },
        "to_date": "2022-03-24"
        # {
        #     "propertyName": "to_date",
        #     "operator": "BETWEEN",
        #     "highValue": "YYYY-MM-DD",
        #     "value": "YYYY-MM-DD",
        # },
    }
    yield variables


def test_velux_club_to_df(var_dictionary):
    source = var_dictionary["source"]
    from_date = var_dictionary["from_date"]
    to_date = var_dictionary["to_date"]

    vc_to_df = VeluxClubToDF(
        source=source,
        to_date=to_date,
        from_date=from_date,
    )
    df = vc_to_df.run()

    assert isinstance(df, pd.DataFrame)
