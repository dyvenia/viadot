import json

from viadot.sources import Eurostat


class EurostatMock(Eurostat):
    def _download_json(self):
        with open("test_eurostat_response.json") as file:
            data = json.load(file)
            return data


def test_eurostat_dictionary_to_df():
    e = EurostatMock()
    data = e._download_json()

    result_df = Eurostat().eurostat_dictionary_to_df(["geo", "time"], data)

    assert list(result_df.columns) == ["geo", "time", "indicator"]

    expected_years = ["2020", "2021", "2022"]
    assert result_df["time"].unique().tolist() == expected_years

    expected_geo = ["Germany", "France", "Italy"]
    assert result_df["geo"].unique().tolist() == expected_geo

    expected_indicator = [
        100,
        150,
        200,
        110,
        160,
        210,
        120,
        170,
        220,
    ]
    assert result_df["indicator"].unique().tolist() == expected_indicator
