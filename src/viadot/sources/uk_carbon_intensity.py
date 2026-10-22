"""UK Carbon Intensity connector."""

from collections.abc import Iterator
from typing import Any

import requests

from viadot.sources.base import Source


class UKCarbonIntensity(Source):
    """Fetch data of Carbon Intensity of the UK Power Grid.

    Documentation for this source API is located at:
    https://carbon-intensity.github.io/api-definitions/#carbon-intensity-api-v2-0-0
    """

    API_ENDPOINT = "https://api.carbonintensity.org.uk"

    def _get(self, api_url: str) -> dict:
        """Fetch the raw response from the Carbon Intensity API."""
        url = f"{self.API_ENDPOINT}{api_url}"
        headers = {"Accept": "application/json"}
        response = requests.get(url, headers=headers, timeout=10)
        if response.ok:
            return response.json()
        msg = f"Error {response.json()}"
        raise ValueError(msg)

    def _to_records(
        self,
        batch_size: int | None = None,  # noqa: ARG002
        api_url: str = "/intensity",
    ) -> Iterator[list[dict[str, Any]]]:
        """Fetch and flatten carbon-intensity measurements into records.

        The base ``Source`` class handles batching, empty responses, and conversion
        to Arrow or pandas. This connector only needs to normalize the API response.
        """
        json_data = self._get(api_url=api_url)
        records = []

        for row in (json_data or {}).get("data", []):
            intensity = row["intensity"]
            record = {
                "from": row["from"],
                "to": row["to"],
                "index": intensity["index"],
            }
            # /intensity endpoint handling.
            if "forecast" in intensity and "actual" in intensity:
                record.update(
                    forecast=intensity["forecast"],
                    actual=intensity["actual"],
                )
            # Historical endpoints handling (/intensity/date/...).
            else:
                record.update(
                    max=intensity["max"],
                    average=intensity["average"],
                    min=intensity["min"],
                )
            records.append(record)

        yield records
