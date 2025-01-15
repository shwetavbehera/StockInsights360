import requests
import json
import os

class ETLController:

    def __init__(self):
        self.api_url = os.getenv('API_URL')
        self.api_key = os.getenv('DATA_API_KEY')


    def fetch_data_time_series_intraday(self, symbol, interval):
            url = f"{self.api_url}/query?function=TIME_SERIES_INTRADAY&symbol={symbol}&interval={interval}&apikey={self.api_key}"
            response = requests.get(url)
            if response.status_code == 200:
                return response.json()
            else:
                raise Exception(f"API request failed with status code {response.status_code}")


if __name__ == "__main__":
    etl = ETLController()
    print(etl.api_url)
    data = etl.fetch_data_time_series_intraday("IBM", "60min")
    print(json.dumps(data, indent=2))