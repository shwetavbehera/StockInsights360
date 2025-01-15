from ETLController import ETLController

def run_etl():
    etl = ETLController()
    db_name = 'stocks.db'
    table_name = 'time_series_intraday'
    columns = ['Timestamp', 'Symbol', 'Interval', 'Open', 'High', 'Low', 'Close', 'Volume']
    interval = '60min'
    symbol = 'IBM'
    query = f"CREATE TABLE IF NOT EXISTS {table_name} (Timestamp TEXT, Symbol TEXT, Interval TEXT, Open REAL, High REAL, Low REAL, Close REAL, Volume INTEGER)"
    etl.create_db(db_name)
    etl.create_table(db_name, query)
    data = etl.fetch_data_time_series_intraday(symbol, interval)
    cleaned_data = etl.clean_data_time_series_intraday(data)
    etl.insert_data(db_name, table_name, columns, cleaned_data)