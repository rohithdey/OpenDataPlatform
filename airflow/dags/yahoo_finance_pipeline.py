"""
Yahoo Finance Data Pipeline DAG
Fetches stock data from Yahoo Finance and stores in DuckDB with Iceberg-style versioning.

Uses yf.download() which is more reliable than Ticker.history() for bulk fetches.
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os
import pandas as pd

# Configuration
DATA_DIR = '/opt/airflow/data'
DUCKDB_PATH = '/opt/airflow/data/warehouse.duckdb'
ICEBERG_DIR = '/opt/airflow/data/iceberg/stock_prices/data'

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}


def fetch_yahoo_finance_data(**context):
    """
    Fetch stock data from Yahoo Finance using yf.download().
    This method is more reliable than Ticker.history() for bulk fetches.
    """
    import yfinance as yf
    import time

    # Get parameters from DAG config or use defaults
    dag_run = context.get('dag_run')
    conf = dag_run.conf if dag_run and dag_run.conf else {}

    symbols = conf.get('symbols', ['AAPL', 'GOOGL', 'MSFT', 'AMZN', 'META'])
    period = conf.get('period', '1mo')

    if isinstance(symbols, str):
        symbols = [s.strip().upper() for s in symbols.split(',')]

    print(f"[Yahoo Finance] Fetching data for symbols: {symbols}")
    print(f"[Yahoo Finance] Period: {period}")

    all_data = []

    # Method 1: Try yf.download() with all symbols at once (faster, more reliable)
    try:
        print("[Yahoo Finance] Attempting bulk download...")

        # yf.download() returns multi-index columns when downloading multiple symbols
        df = yf.download(
            tickers=symbols,
            period=period,
            group_by='ticker',
            auto_adjust=True,
            progress=False,
            threads=True
        )

        if not df.empty:
            print(f"[Yahoo Finance] Bulk download successful, processing {len(df)} rows")

            # Handle single vs multiple symbols (different DataFrame structure)
            if len(symbols) == 1:
                # Single symbol: columns are just OHLCV
                symbol = symbols[0]
                df = df.reset_index()
                df['symbol'] = symbol
                df['fetch_timestamp'] = datetime.now().isoformat()
                df.columns = [c.lower().replace(' ', '_') for c in df.columns]
                all_data.append(df)
            else:
                # Multiple symbols: columns are multi-index (symbol, metric)
                for symbol in symbols:
                    try:
                        if symbol in df.columns.get_level_values(0):
                            symbol_df = df[symbol].copy()
                            symbol_df = symbol_df.reset_index()
                            symbol_df['symbol'] = symbol
                            symbol_df['fetch_timestamp'] = datetime.now().isoformat()
                            symbol_df.columns = [c.lower().replace(' ', '_') for c in symbol_df.columns]

                            # Drop rows with all NaN values
                            symbol_df = symbol_df.dropna(subset=['open', 'high', 'low', 'close'], how='all')

                            if not symbol_df.empty:
                                all_data.append(symbol_df)
                                print(f"[Yahoo Finance] Got {len(symbol_df)} records for {symbol}")
                            else:
                                print(f"[Yahoo Finance] No data for {symbol}")
                    except Exception as e:
                        print(f"[Yahoo Finance] Error processing {symbol}: {e}")

    except Exception as e:
        print(f"[Yahoo Finance] Bulk download failed: {e}")

    # Method 2: Fallback to individual downloads if bulk failed
    if not all_data:
        print("[Yahoo Finance] Falling back to individual symbol downloads...")

        for symbol in symbols:
            try:
                print(f"[Yahoo Finance] Fetching {symbol} individually...")

                df = yf.download(
                    tickers=symbol,
                    period=period,
                    auto_adjust=True,
                    progress=False
                )

                if not df.empty:
                    df = df.reset_index()
                    df['symbol'] = symbol
                    df['fetch_timestamp'] = datetime.now().isoformat()
                    df.columns = [c.lower().replace(' ', '_') for c in df.columns]
                    all_data.append(df)
                    print(f"[Yahoo Finance] Got {len(df)} records for {symbol}")
                else:
                    print(f"[Yahoo Finance] No data for {symbol}")

                # Rate limiting
                time.sleep(1)

            except Exception as e:
                print(f"[Yahoo Finance] Error fetching {symbol}: {e}")
                time.sleep(2)

    if not all_data:
        raise ValueError(f"No data fetched from Yahoo Finance for symbols: {symbols}")

    # Combine all data
    combined_df = pd.concat(all_data, ignore_index=True)

    # Ensure consistent column names
    column_mapping = {
        'date': 'date',
        'datetime': 'date',
        'open': 'open',
        'high': 'high',
        'low': 'low',
        'close': 'close',
        'adj_close': 'adj_close',
        'volume': 'volume',
        'symbol': 'symbol',
        'fetch_timestamp': 'fetch_timestamp'
    }

    # Rename columns that exist
    for old_name, new_name in column_mapping.items():
        if old_name in combined_df.columns and old_name != new_name:
            combined_df = combined_df.rename(columns={old_name: new_name})

    print(f"[Yahoo Finance] Total records fetched: {len(combined_df)}")
    print(f"[Yahoo Finance] Columns: {list(combined_df.columns)}")

    # Push to XCom as JSON
    return combined_df.to_json(orient='records', date_format='iso')


def save_to_iceberg_style(**context):
    """
    Save data to timestamped Parquet files (Iceberg-style versioning).
    """
    import json
    import pyarrow as pa
    import pyarrow.parquet as pq

    ti = context['ti']
    json_data = ti.xcom_pull(task_ids='fetch_yahoo_data')

    if not json_data:
        raise ValueError("No data received from fetch task")

    df = pd.read_json(json_data, orient='records')
    print(f"[Iceberg] Processing {len(df)} records")

    # Create directory
    os.makedirs(ICEBERG_DIR, exist_ok=True)

    # Generate timestamped filename
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    snapshot_id = int(datetime.now().timestamp() * 1000)
    parquet_path = os.path.join(ICEBERG_DIR, f'snapshot_{snapshot_id}_{timestamp}.parquet')

    # Save as Parquet
    table = pa.Table.from_pandas(df)
    pq.write_table(table, parquet_path, compression='snappy')

    print(f"[Iceberg] Saved snapshot: {parquet_path}")
    print(f"[Iceberg] File size: {os.path.getsize(parquet_path) / 1024:.2f} KB")

    # Update metadata
    metadata_path = os.path.join(ICEBERG_DIR, '..', 'metadata.json')
    try:
        if os.path.exists(metadata_path):
            with open(metadata_path, 'r') as f:
                metadata = json.load(f)
        else:
            metadata = {'snapshots': [], 'current_snapshot_id': None}
    except:
        metadata = {'snapshots': [], 'current_snapshot_id': None}

    snapshot_info = {
        'snapshot_id': snapshot_id,
        'timestamp': timestamp,
        'file_path': parquet_path,
        'record_count': len(df),
        'symbols': df['symbol'].unique().tolist() if 'symbol' in df.columns else [],
    }

    metadata['snapshots'].append(snapshot_info)
    metadata['current_snapshot_id'] = snapshot_id

    if len(metadata['snapshots']) > 100:
        metadata['snapshots'] = metadata['snapshots'][-100:]

    with open(metadata_path, 'w') as f:
        json.dump(metadata, f, indent=2)

    return parquet_path


def load_to_duckdb(**context):
    """
    Load the latest snapshot into DuckDB.
    """
    import duckdb
    import time

    ti = context['ti']
    parquet_path = ti.xcom_pull(task_ids='save_to_iceberg')

    if not parquet_path or not os.path.exists(parquet_path):
        raise ValueError(f"Parquet file not found: {parquet_path}")

    print(f"[DuckDB] Loading from: {parquet_path}")

    max_retries = 5
    retry_delay = 2

    for attempt in range(max_retries):
        conn = None
        try:
            conn = duckdb.connect(DUCKDB_PATH, read_only=False)

            # Create table with flexible schema
            conn.execute("""
                CREATE TABLE IF NOT EXISTS stock_prices (
                    date TIMESTAMP,
                    open DOUBLE,
                    high DOUBLE,
                    low DOUBLE,
                    close DOUBLE,
                    volume DOUBLE,
                    symbol VARCHAR,
                    fetch_timestamp VARCHAR,
                    ingestion_date DATE DEFAULT CURRENT_DATE
                )
            """)

            # Read parquet and get actual columns
            parquet_df = conn.execute(f"SELECT * FROM read_parquet('{parquet_path}') LIMIT 1").fetchdf()
            available_cols = list(parquet_df.columns)
            print(f"[DuckDB] Parquet columns: {available_cols}")

            # Get table columns to handle schema mismatch
            table_cols = conn.execute("SELECT * FROM stock_prices LIMIT 0").description
            table_col_names = [col[0] for col in table_cols]
            print(f"[DuckDB] Table columns: {table_col_names}")

            # Build fully dynamic INSERT - only use columns that exist in BOTH
            base_cols = ['date', 'open', 'high', 'low', 'close', 'volume', 'symbol']
            insert_cols = []
            select_parts = []

            for col in base_cols:
                if col in table_col_names:
                    insert_cols.append(col)
                    if col in available_cols:
                        select_parts.append(f'"{col}"')
                    else:
                        select_parts.append('NULL')

            # Add optional columns only if they exist in the table
            if 'fetch_timestamp' in table_col_names:
                insert_cols.append('fetch_timestamp')
                if 'fetch_timestamp' in available_cols:
                    select_parts.append('"fetch_timestamp"')
                else:
                    select_parts.append('NULL')

            if 'ingestion_date' in table_col_names:
                insert_cols.append('ingestion_date')
                select_parts.append('CURRENT_DATE')

            insert_clause = ", ".join(insert_cols)
            select_clause = ", ".join(select_parts)

            conn.execute(f"""
                INSERT INTO stock_prices ({insert_clause})
                SELECT {select_clause}
                FROM read_parquet('{parquet_path}')
            """)

            count = conn.execute("SELECT COUNT(*) FROM stock_prices").fetchone()[0]
            print(f"[DuckDB] Total records in stock_prices: {count}")

            conn.close()
            return count

        except duckdb.IOException as e:
            if "lock" in str(e).lower() and attempt < max_retries - 1:
                print(f"[DuckDB] Database locked, retrying in {retry_delay}s")
                if conn:
                    conn.close()
                time.sleep(retry_delay)
                retry_delay *= 2
            else:
                if conn:
                    conn.close()
                raise
        except Exception as e:
            if conn:
                conn.close()
            raise


def create_stock_views(**context):
    """
    Create useful views for analyzing stock data.
    """
    import duckdb

    conn = duckdb.connect(DUCKDB_PATH, read_only=False)

    conn.execute("""
        CREATE OR REPLACE VIEW stock_daily_returns AS
        SELECT
            symbol,
            date,
            close,
            LAG(close) OVER (PARTITION BY symbol ORDER BY date) as prev_close,
            (close - LAG(close) OVER (PARTITION BY symbol ORDER BY date)) /
                NULLIF(LAG(close) OVER (PARTITION BY symbol ORDER BY date), 0) * 100 as daily_return_pct
        FROM stock_prices
        ORDER BY symbol, date
    """)

    conn.execute("""
        CREATE OR REPLACE VIEW stock_latest_prices AS
        SELECT
            symbol,
            date as last_date,
            open,
            high,
            low,
            close,
            volume
        FROM stock_prices
        WHERE (symbol, date) IN (
            SELECT symbol, MAX(date)
            FROM stock_prices
            GROUP BY symbol
        )
    """)

    conn.execute("""
        CREATE OR REPLACE VIEW stock_summary AS
        SELECT
            symbol,
            COUNT(*) as data_points,
            MIN(date) as first_date,
            MAX(date) as last_date,
            AVG(close) as avg_close,
            MIN(close) as min_close,
            MAX(close) as max_close,
            AVG(volume) as avg_volume
        FROM stock_prices
        GROUP BY symbol
    """)

    print("[DuckDB] Created views: stock_daily_returns, stock_latest_prices, stock_summary")
    conn.close()


# DAG Definition
with DAG(
    'yahoo_finance_daily',
    default_args=default_args,
    description='Fetch stock prices from Yahoo Finance with Iceberg-style versioning',
    schedule_interval='0 18 * * 1-5',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['yahoo-finance', 'stocks', 'market-data'],
    params={
        'symbols': 'AAPL,GOOGL,MSFT,AMZN,META',
        'period': '1mo'
    }
) as dag:

    dag.doc_md = """
    ## Yahoo Finance Stock Data Pipeline

    Fetches stock price data using yf.download() which is more reliable than Ticker.history().

    ### Trigger with config:
    ```json
    {"symbols": "AAPL,GOOGL,TSLA", "period": "1mo"}
    ```

    ### Period options: 1d, 5d, 1mo, 3mo, 6mo, 1y, 2y, 5y, max
    """

    fetch_task = PythonOperator(
        task_id='fetch_yahoo_data',
        python_callable=fetch_yahoo_finance_data,
        provide_context=True,
    )

    save_task = PythonOperator(
        task_id='save_to_iceberg',
        python_callable=save_to_iceberg_style,
        provide_context=True,
    )

    load_task = PythonOperator(
        task_id='load_to_duckdb',
        python_callable=load_to_duckdb,
        provide_context=True,
    )

    views_task = PythonOperator(
        task_id='create_views',
        python_callable=create_stock_views,
        provide_context=True,
    )

    fetch_task >> save_task >> load_task >> views_task
