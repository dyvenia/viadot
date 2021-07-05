import pandas as pd
import requests

from .base import Source


class UKCarbonIntensity(Source):
    """Fetches data of Carbon Intensity of the UK Power Grid.

    Documentation for this source API is located
    at: https://carbon-intensity.github.io/api-definitions/#carbon-intensity-api-v2-0-0

    Parameters
    ----------
    api_url : str, optional
        The URL endpoint to call, by default None
    """

    def __init__(self, *args, api_url: str = None, **kwargs):
        super().__init__(*args, **kwargs)
        self.api_url = api_url
        self.API_ENDPOINT = "https://api.carbonintensity.org.uk"

    def to_json(self):
        url = f"{self.API_ENDPOINT}{self.api_url}"
        headers = {"Accept": "application/json"}
        response = requests.get(url, params={}, headers=headers)
        if response.ok:
            return response.json()
        else:
            raise f"Error {response.json()}"

    def to_df(self, if_empty: str = "warn"):
        """Returns a pandas DataFrame with flattened data

        Returns:
            pandas.DataFrame: A Pandas DataFrame
        """
        from_ = []
        to = []
        forecast = []
        actual = []
        max_ = []
        average = []
        min_ = []
        index = []
        json_data = self.to_json()

        if not json_data:
            self._handle_if_empty(if_empty)

        for row in json_data["data"]:
            from_.append(row["from"])
            to.append(row["to"])
            index.append(row["intensity"]["index"])
            try:
                forecast.append(row["intensity"]["forecast"])
                actual.append(row["intensity"]["actual"])
                df = pd.DataFrame(
                    {
                        "from": from_,
                        "to": to,
                        "forecast": forecast,
                        "actual": actual,
                        "index": index,
                    }
                )
            except KeyError:
                max_.append(row["intensity"]["max"])
                average.append(row["intensity"]["average"])
                min_.append(row["intensity"]["min"])
                df = pd.DataFrame(
                    {
                        "from": from_,
                        "to": to,
                        "max": max_,
                        "average": average,
                        "min": min_,
                    }
                )
            return df

    def query(self, api_url: str):
        self.api_url = api_url
        return True
