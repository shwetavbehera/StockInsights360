from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import sqlite3

DB_NAME = 'StockInsight360.db'
TABLE_NAME = 'time_series_intraday'

# Function to create a new SQLite database
def create_db():
    """
    Creates an SQLite database with the specified name.
    """
    conn = sqlite3.connect(DB_NAME)
    conn.close()

# Function to create a table in the SQLite database
def create_table():
    """
    Creates a table in the SQLite database for storing time series intraday data.
    If the table already exists, this function does nothing.
    """
    query = f"""
    CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
        Timestamp TEXT PRIMARY KEY,
        Symbol TEXT,
        Interval TEXT,
        Open REAL,
        High REAL,
        Low REAL,
        Close REAL,
        Volume INTEGER
    )
    """
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    cursor.execute(query)
    conn.commit()
    conn.close()

# DAG to handle one-time setup for database and table creation
with DAG(
    'setup_database',
    description='One-time setup for database and table creation',
    schedule_interval=None,  # This DAG runs only when triggered manually
    start_date=datetime(2025, 1, 1),
    catchup=False,
) as dag:
    
    task_create_db = PythonOperator(
        task_id='create_db',
        python_callable=create_db,
    )

    task_create_table = PythonOperator(
        task_id='create_table',
        python_callable=create_table,
    )

    task_create_db >> task_create_table
