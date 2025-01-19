from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
import sqlite3
import pandas as pd
import os

API_URL = os.getenv('API_URL')  # API base URL fetched from environment variables
API_KEY = os.getenv('DATA_API_KEY')  # API key fetched from environment variables
DB_NAME = 'data.db'
TABLE_NAME = 'time_series_intraday'

# Function to fetch data from an API
def fetch_data(symbol, interval, **kwargs):
    """
    Fetches time series intraday data from the API.
    Pushes the raw data into Airflow's XCom for downstream tasks.
    
    Args:
    - symbol (str): Stock symbol to fetch data for.
    - interval (str): Interval of time series data (e.g., '60min').
    """
    url = f"{API_URL}/query?function=TIME_SERIES_INTRADAY&symbol={symbol}&interval={interval}&apikey={API_KEY}"
    response = requests.get(url)
    if response.status_code == 200:
        kwargs['ti'].xcom_push(key='raw_data', value=response.json())
    else:
        raise Exception(f"API request failed with status code {response.status_code}")

# Function to clean and transform the raw data
def clean_data(**kwargs):
    """
    Cleans and transforms the raw time series intraday data.
    Converts the raw JSON data into a list of dictionaries and pushes it to XCom.
    
    Args:
    - kwargs (dict): Contains XCom-pulled raw data.
    """
    raw_data = kwargs['ti'].xcom_pull(key='raw_data', task_ids='fetch_data')
    meta_data = raw_data['Meta Data']
    symbol = meta_data['2. Symbol']
    interval = meta_data['4. Interval']

    time_series = raw_data['Time Series (60min)']
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
    df['Timestamp'] = pd.to_datetime(df['Timestamp'])  # Convert Timestamp to a datetime object
    kwargs['ti'].xcom_push(key='cleaned_data', value=df.to_dict(orient='records'))

# Function to load cleaned data into SQLite database
def load_data(**kwargs):
    """
    Loads the cleaned data into the SQLite database.
    Inserts the data into the specified table, ignoring duplicate entries.
    
    Args:
    - kwargs (dict): Contains XCom-pulled cleaned data.
    """
    cleaned_data = kwargs['ti'].xcom_pull(key='cleaned_data', task_ids='clean_data')
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()

    columns = ['Timestamp', 'Symbol', 'Interval', 'Open', 'High', 'Low', 'Close', 'Volume']
    cursor.executemany(
        f"INSERT OR IGNORE INTO {TABLE_NAME} ({', '.join(columns)}) VALUES ({', '.join(['?'] * len(columns))})",
        [tuple(d[col] for col in columns) for d in cleaned_data]
    )
    conn.commit()
    conn.close()

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# DAG definition for recurring ETL process
with DAG(
    'etl_time_series_intraday',
    default_args=default_args,
    description='ETL pipeline for time series intraday data',
    schedule_interval=timedelta(hours=1),  # Schedule to run every hour
    start_date=datetime(2025, 1, 1),
    catchup=False,
) as dag:
    
    task_fetch_data = PythonOperator(
        task_id='fetch_data',
        python_callable=fetch_data,
        op_kwargs={'symbol': 'IBM', 'interval': '60min'},
    )

    task_clean_data = PythonOperator(
        task_id='clean_data',
        python_callable=clean_data,
    )

    task_load_data = PythonOperator(
        task_id='load_data',
        python_callable=load_data,
    )

    # Define task dependencies
    task_fetch_data >> task_clean_data >> task_load_data
