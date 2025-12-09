"""
Yahoo Finance Data Pipeline DAG
Fetches stock data from Yahoo Finance and stores in DuckDB with Iceberg-style versioning.

Features:
- User-Agent headers to avoid blocking
- Timestamped Parquet files for version history (Iceberg-style)
- Retry logic with exponential backoff
- Data validation before storage
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os
import pandas as pd
import requests

# Configuration
DATA_DIR = '/opt/airflow/data'
DUCKDB_PATH = '/opt/airflow/data/warehouse.duckdb'
ICEBERG_DIR = '/opt/airflow/data/iceberg/stock_prices/data'

# User-Agent to avoid Yahoo blocking
USER_AGENT = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=2),
}


def fetch_yahoo_finance_data(**context):
    """
    Fetch stock data from Yahoo Finance using yfinance library.
    Uses proper User-Agent headers to avoid being blocked.
    """
    import yfinance as yf
    import time

    # Get parameters from DAG config or use defaults
    dag_run = context.get('dag_run')
    conf = dag_run.conf if dag_run and dag_run.conf else {}

    symbols = conf.get('symbols', ['AAPL', 'GOOGL', 'MSFT', 'AMZN', 'META'])
    period = conf.get('period', '1mo')  # 1d, 5d, 1mo, 3mo, 6mo, 1y, 2y, 5y, 10y, ytd, max

    if isinstance(symbols, str):
        symbols = [s.strip() for s in symbols.split(',')]

    print(f"[Yahoo Finance] Fetching data for symbols: {symbols}")
    print(f"[Yahoo Finance] Period: {period}")

    # Create a session with proper headers
    session = requests.Session()
    session.headers.update({
        'User-Agent': USER_AGENT,
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.5',
        'Accept-Encoding': 'gzip, deflate',
        'DNT': '1',
        'Connection': 'keep-alive',
        'Upgrade-Insecure-Requests': '1',
    })

    all_data = []
    failed_symbols = []

    for symbol in symbols:
        print(f"[Yahoo Finance] Fetching {symbol}...")

        # Retry logic
        max_retries = 3
        for attempt in range(max_retries):
            try:
                ticker = yf.Ticker(symbol, session=session)
                hist = ticker.history(period=period)

                if hist.empty:
                    print(f"[Yahoo Finance] WARNING: No data returned for {symbol}")
                    if attempt < max_retries - 1:
                        time.sleep(2 ** attempt)  # Exponential backoff
                        continue
                    failed_symbols.append(symbol)
                    break

                # Reset index to get Date as a column
                hist = hist.reset_index()
                hist['symbol'] = symbol
                hist['fetch_timestamp'] = datetime.now().isoformat()

                # Rename columns to be more DuckDB-friendly
                hist.columns = [c.lower().replace(' ', '_') for c in hist.columns]

                all_data.append(hist)
                print(f"[Yahoo Finance] Got {len(hist)} records for {symbol}")
                break

            except Exception as e:
                print(f"[Yahoo Finance] Error fetching {symbol} (attempt {attempt + 1}): {e}")
                if attempt < max_retries - 1:
                    time.sleep(2 ** attempt)
                else:
                    failed_symbols.append(symbol)

        # Be polite to Yahoo's servers
        time.sleep(0.5)

    if not all_data:
        raise ValueError(f"No data fetched from Yahoo Finance. Failed symbols: {failed_symbols}")

    # Combine all data
    df = pd.concat(all_data, ignore_index=True)
    print(f"[Yahoo Finance] Total records fetched: {len(df)}")

    # Push to XCom as JSON (for Airflow)
    return df.to_json(orient='records', date_format='iso')


def save_to_iceberg_style(**context):
    """
    Save data to timestamped Parquet files (Iceberg-style versioning).
    Each run creates a new snapshot file.
    """
    import json
    import pyarrow as pa
    import pyarrow.parquet as pq

    # Get data from previous task
    ti = context['ti']
    json_data = ti.xcom_pull(task_ids='fetch_yahoo_data')

    if not json_data:
        raise ValueError("No data received from fetch task")

    df = pd.read_json(json_data, orient='records')
    print(f"[Iceberg] Processing {len(df)} records")

    # Create Iceberg-style directory structure
    os.makedirs(ICEBERG_DIR, exist_ok=True)

    # Generate timestamped filename (Iceberg snapshot style)
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    snapshot_id = int(datetime.now().timestamp() * 1000)
    parquet_path = os.path.join(ICEBERG_DIR, f'snapshot_{snapshot_id}_{timestamp}.parquet')

    # Convert to PyArrow table and save as Parquet
    table = pa.Table.from_pandas(df)
    pq.write_table(table, parquet_path, compression='snappy')

    print(f"[Iceberg] Saved snapshot: {parquet_path}")
    print(f"[Iceberg] File size: {os.path.getsize(parquet_path) / 1024:.2f} KB")

    # Create/update metadata file (simple manifest)
    metadata_path = os.path.join(ICEBERG_DIR, '..', 'metadata.json')

    try:
        if os.path.exists(metadata_path):
            with open(metadata_path, 'r') as f:
                metadata = json.load(f)
        else:
            metadata = {'snapshots': [], 'current_snapshot_id': None}
    except:
        metadata = {'snapshots': [], 'current_snapshot_id': None}

    # Add new snapshot
    snapshot_info = {
        'snapshot_id': snapshot_id,
        'timestamp': timestamp,
        'file_path': parquet_path,
        'record_count': len(df),
        'symbols': df['symbol'].unique().tolist() if 'symbol' in df.columns else [],
    }

    metadata['snapshots'].append(snapshot_info)
    metadata['current_snapshot_id'] = snapshot_id

    # Keep only last 100 snapshots in metadata
    if len(metadata['snapshots']) > 100:
        metadata['snapshots'] = metadata['snapshots'][-100:]

    with open(metadata_path, 'w') as f:
        json.dump(metadata, f, indent=2)

    return parquet_path


def load_to_duckdb(**context):
    """
    Load the latest snapshot into DuckDB for querying.
    Creates/updates the stock_prices table.
    """
    import duckdb
    import time

    # Get the parquet path from previous task
    ti = context['ti']
    parquet_path = ti.xcom_pull(task_ids='save_to_iceberg')

    if not parquet_path or not os.path.exists(parquet_path):
        raise ValueError(f"Parquet file not found: {parquet_path}")

    print(f"[DuckDB] Loading from: {parquet_path}")

    # Retry logic for database lock
    max_retries = 5
    retry_delay = 2

    for attempt in range(max_retries):
        try:
            conn = duckdb.connect(DUCKDB_PATH, read_only=False)

            # Create table if not exists
            conn.execute("""
                CREATE TABLE IF NOT EXISTS stock_prices (
                    date TIMESTAMP,
                    open DOUBLE,
                    high DOUBLE,
                    low DOUBLE,
                    close DOUBLE,
                    volume BIGINT,
                    dividends DOUBLE,
                    stock_splits DOUBLE,
                    symbol VARCHAR,
                    fetch_timestamp VARCHAR,
                    ingestion_date DATE DEFAULT CURRENT_DATE
                )
            """)

            # Load new data (append mode - keep historical)
            conn.execute(f"""
                INSERT INTO stock_prices
                SELECT
                    date,
                    open,
                    high,
                    low,
                    close,
                    volume,
                    dividends,
                    stock_splits,
                    symbol,
                    fetch_timestamp,
                    CURRENT_DATE as ingestion_date
                FROM read_parquet('{parquet_path}')
            """)

            # Get count
            count = conn.execute("SELECT COUNT(*) FROM stock_prices").fetchone()[0]
            print(f"[DuckDB] Total records in stock_prices: {count}")

            conn.close()
            return count

        except duckdb.IOException as e:
            if "lock" in str(e).lower() and attempt < max_retries - 1:
                print(f"[DuckDB] Database locked, retrying in {retry_delay}s (attempt {attempt + 1})")
                time.sleep(retry_delay)
                retry_delay *= 2
            else:
                raise
        except Exception as e:
            if 'conn' in locals():
                conn.close()
            raise


def create_stock_views(**context):
    """
    Create useful views for analyzing stock data.
    """
    import duckdb

    conn = duckdb.connect(DUCKDB_PATH, read_only=False)

    # Daily returns view
    conn.execute("""
        CREATE OR REPLACE VIEW stock_daily_returns AS
        SELECT
            symbol,
            date,
            close,
            LAG(close) OVER (PARTITION BY symbol ORDER BY date) as prev_close,
            (close - LAG(close) OVER (PARTITION BY symbol ORDER BY date)) /
                LAG(close) OVER (PARTITION BY symbol ORDER BY date) * 100 as daily_return_pct
        FROM stock_prices
        ORDER BY symbol, date
    """)

    # Latest prices view
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

    # Summary statistics view
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
    schedule_interval='0 18 * * 1-5',  # 6 PM on weekdays (after market close)
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

    Fetches stock price data from Yahoo Finance and stores it with Iceberg-style versioning.

    ### Configuration

    You can override the defaults when triggering:
    ```json
    {
        "symbols": "AAPL,GOOGL,TSLA",
        "period": "1mo"
    }
    ```

    ### Period Options
    - `1d` - 1 day
    - `5d` - 5 days
    - `1mo` - 1 month
    - `3mo` - 3 months
    - `6mo` - 6 months
    - `1y` - 1 year
    - `2y` - 2 years
    - `5y` - 5 years
    - `max` - All available data

    ### Tables Created
    - `stock_prices` - Raw price data
    - `stock_daily_returns` - View with daily returns
    - `stock_latest_prices` - View with most recent prices
    - `stock_summary` - View with summary statistics

    ### Iceberg-Style Versioning
    Each run creates a timestamped Parquet file in:
    `/opt/airflow/data/iceberg/stock_prices/data/`

    Query historical snapshots with:
    ```sql
    SELECT * FROM read_parquet('/opt/airflow/data/iceberg/stock_prices/data/*.parquet')
    ```
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
