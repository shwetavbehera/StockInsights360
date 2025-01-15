import requests
import json
import os
import sqlite3
import pandas as pd
class ETLController:

    def __init__(self):
        self.api_url = os.getenv('API_URL')
        self.api_key = os.getenv('DATA_API_KEY')
    
    # Helper methods
    def create_db(self, db_name):
        conn = sqlite3.connect(db_name)
        conn.close()
    
    def create_table(self, db_name, query):
        conn = sqlite3.connect(db_name)
        cursor = conn.cursor()
        cursor.execute(query)
        conn.commit()
        conn.close()

    # Extract methods
    def fetch_data_time_series_intraday(self, symbol, interval):
            url = f"{self.api_url}/query?function=TIME_SERIES_INTRADAY&symbol={symbol}&interval={interval}&apikey={self.api_key}"
            response = requests.get(url)
            if response.status_code == 200:
                return response.json()
            else:
                raise Exception(f"API request failed with status code {response.status_code}")
    
    # Transform methods
    def clean_data_time_series_intraday(self, data):
        meta_data = data['Meta Data']
        symbol = meta_data['2. Symbol']
        interval = meta_data['4. Interval']

        time_series = data['Time Series (60min)']
        records = []
        for timestamp, data in time_series.items():
            record = {
                'Timestamp': timestamp,
                'Symbol': symbol,
                'Interval': interval,
                'Open': float(data['1. open']),
                'High': float(data['2. high']),
                'Low': float(data['3. low']),
                'Close': float(data['4. close']),
                'Volume': int(data['5. volume']),
            }
            records.append(record)
            df = pd.DataFrame(records)
            df['Timestamp'] = pd.to_datetime(df['Timestamp'])
            return df

    # Load methods
    def insert_data(self, db_name, table_name, columns, data):
        conn = sqlite3.connect(db_name)
        cursor = conn.cursor()
        cursor = conn.executemany(f"INSERT INTO {table_name} ({','.join(columns)}) VALUES ({','.join(['?']*len(columns))})", data.values().tolist())
        conn.commit()
        conn.close()

if __name__ == "__main__":
    etl = ETLController()
    print(etl.api_url)
    data = etl.fetch_data_time_series_intraday("IBM", "60min")
    print(json.dumps(data, indent=2))