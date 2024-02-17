import json, os, sys
import requests
import dlt


class APIGatherer:
    """Base class for gathering data from APIs."""

    def __init__(self, url: str, limit: str = "1000"):
        self.url = url
        self.limit = limit
        self.offset = "0"

    def _fetch_data(self) -> dict:
        """Fetches data from the API and handles errors."""
        try:
            response = requests.get(self.url, stream=True, params={"$limit": self.limit, "$offset": self.offset})
            response.raise_for_status()
            return response.json()
        except requests.exceptions.HTTPError:
            raise RuntimeError(f"Failed to fetch data from {self.url}")

    def gather_data(self) -> Iterator[dict]:
        """Yields data chunks in a continuous stream."""
        while True:
            data = self._fetch_data()
            yield data

            if self.offset == "3000": 
                break

            self.offset = str(int(self.offset) + int(self.limit))
            print(f"{self.url.split('/')[-1]}:", self.offset)  

class AirAPIGatherer(APIGatherer):
    """Gatherer for air quality API."""

    def __init__(self):
        super().__init__(url="https://data.calgary.ca/resource/g9s5-qhu5.json")

class WaterAPIGatherer(APIGatherer):
    """Gatherer for water quality API."""

    def __init__(self):
        super().__init__(url="https://data.calgary.ca/resource/y8as-bmzj.json")


## File run method
@dlt.source
def gather_api_data():
    return [AirAPIGatherer(), WaterAPIGatherer()]  
