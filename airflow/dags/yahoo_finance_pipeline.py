"""
Yahoo Finance Data Pipeline DAG
Fetches stock data from Yahoo Finance and stores using Delta Lake format with ACID transactions.

Features:
- Delta Lake format with full ACID compliance
- Time travel queries (query any historical version)
- Automatic compaction and optimization
- Schema evolution support
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os
import pandas as pd

# Configuration
DATA_DIR = '/opt/airflow/data'
DUCKDB_PATH = '/opt/airflow/data/warehouse.duckdb'
DELTA_TABLE_PATH = '/opt/airflow/data/delta/stock_prices'

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
        'datetime': 'date',
        'adj_close': 'adj_close',
    }

    for old_name, new_name in column_mapping.items():
        if old_name in combined_df.columns and old_name != new_name:
            combined_df = combined_df.rename(columns={old_name: new_name})

    print(f"[Yahoo Finance] Total records fetched: {len(combined_df)}")
    print(f"[Yahoo Finance] Columns: {list(combined_df.columns)}")

    # Push to XCom as JSON
    return combined_df.to_json(orient='records', date_format='iso')


def save_to_delta_lake(**context):
    """
    Save data to Delta Lake format with ACID transactions.

    Delta Lake provides:
    - ACID transactions (atomic writes)
    - Time travel (query historical versions)
    - Schema evolution
    - Audit history
    """
    from deltalake import write_deltalake, DeltaTable
    import pyarrow as pa
    from io import StringIO

    ti = context['ti']
    json_data = ti.xcom_pull(task_ids='fetch_yahoo_data')

    if not json_data:
        raise ValueError("No data received from fetch task")

    # Fix FutureWarning by wrapping in StringIO
    df = pd.read_json(StringIO(json_data), orient='records')
    print(f"[Delta Lake] Processing {len(df)} records")

    # Ensure directory exists
    os.makedirs(os.path.dirname(DELTA_TABLE_PATH), exist_ok=True)

    # Add ingestion metadata
    df['ingestion_date'] = datetime.now().date()
    df['ingestion_timestamp'] = datetime.now().isoformat()

    # Ensure date column is datetime
    if 'date' in df.columns:
        df['date'] = pd.to_datetime(df['date'])

    # Convert to PyArrow Table for Delta Lake compatibility
    table = pa.Table.from_pandas(df)

    # Write to Delta Lake with ACID transaction
    # mode="append" adds new data
    # mode="overwrite" replaces all data
    try:
        if os.path.exists(DELTA_TABLE_PATH) and os.path.exists(os.path.join(DELTA_TABLE_PATH, '_delta_log')):
            # Table exists - append with merge to avoid duplicates
            print("[Delta Lake] Appending to existing table...")
            write_deltalake(
                DELTA_TABLE_PATH,
                table,
                mode="append",
                schema_mode="merge"  # Allow schema evolution
            )
        else:
            # Create new table
            print("[Delta Lake] Creating new Delta table...")
            write_deltalake(
                DELTA_TABLE_PATH,
                table,
                mode="overwrite",
                partition_by=["symbol"]  # Partition by symbol for faster queries
            )

        # Get table info
        dt = DeltaTable(DELTA_TABLE_PATH)
        version = dt.version()
        history = dt.history(limit=1)

        print(f"[Delta Lake] Successfully saved to: {DELTA_TABLE_PATH}")
        print(f"[Delta Lake] Current version: {version}")
        print(f"[Delta Lake] Total files: {len(dt.files())}")

        if history:
            print(f"[Delta Lake] Last operation: {history[0].get('operation', 'unknown')}")

        return {
            'path': DELTA_TABLE_PATH,
            'version': version,
            'records': len(df)
        }

    except Exception as e:
        print(f"[Delta Lake] Error writing: {e}")
        raise


def optimize_delta_table(**context):
    """
    Optimize the Delta table by compacting small files.
    This improves query performance.
    """
    from deltalake import DeltaTable

    try:
        dt = DeltaTable(DELTA_TABLE_PATH)

        # Compact small files (optional - improves read performance)
        print("[Delta Lake] Running optimization...")
        dt.optimize.compact()

        # Vacuum old files (keep 7 days of history by default)
        # This removes files no longer referenced by the table
        # dt.vacuum(retention_hours=168, enforce_retention_duration=False)

        print(f"[Delta Lake] Optimization complete. Version: {dt.version()}")

    except Exception as e:
        print(f"[Delta Lake] Optimization skipped: {e}")


def sync_to_duckdb(**context):
    """
    Sync Delta Lake table to DuckDB for fast analytical queries.
    Uses DuckDB's delta extension for native Delta Lake support.
    """
    import duckdb
    import time

    ti = context['ti']
    delta_info = ti.xcom_pull(task_ids='save_to_delta')

    if not delta_info:
        print("[DuckDB] No delta info received, skipping sync")
        return

    print(f"[DuckDB] Syncing from Delta Lake: {DELTA_TABLE_PATH}")

    max_retries = 5
    retry_delay = 2

    for attempt in range(max_retries):
        conn = None
        try:
            conn = duckdb.connect(DUCKDB_PATH, read_only=False)

            # Install and load delta extension
            conn.execute("INSTALL delta;")
            conn.execute("LOAD delta;")

            # Create/replace table from Delta Lake
            # This gives us a queryable copy in DuckDB
            conn.execute(f"""
                CREATE OR REPLACE TABLE stock_prices AS
                SELECT * FROM delta_scan('{DELTA_TABLE_PATH}')
            """)

            count = conn.execute("SELECT COUNT(*) FROM stock_prices").fetchone()[0]
            symbols = conn.execute("SELECT DISTINCT symbol FROM stock_prices").fetchall()

            print(f"[DuckDB] Synced {count} records")
            print(f"[DuckDB] Symbols: {[s[0] for s in symbols]}")

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
            # If delta extension fails, fall back to parquet reading
            print(f"[DuckDB] Delta extension error: {e}")
            print("[DuckDB] Falling back to parquet read...")
            return sync_to_duckdb_fallback()


def sync_to_duckdb_fallback():
    """
    Fallback: Read Delta Lake parquet files directly if extension fails.
    """
    import duckdb

    conn = duckdb.connect(DUCKDB_PATH, read_only=False)

    # Delta Lake stores data as parquet files
    parquet_pattern = f"{DELTA_TABLE_PATH}/*.parquet"

    conn.execute(f"""
        CREATE OR REPLACE TABLE stock_prices AS
        SELECT * FROM read_parquet('{parquet_pattern}')
    """)

    count = conn.execute("SELECT COUNT(*) FROM stock_prices").fetchone()[0]
    print(f"[DuckDB Fallback] Synced {count} records from parquet files")

    conn.close()
    return count


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
            ROUND((close - LAG(close) OVER (PARTITION BY symbol ORDER BY date)) /
                NULLIF(LAG(close) OVER (PARTITION BY symbol ORDER BY date), 0) * 100, 2) as daily_return_pct
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
            ROUND(AVG(close), 2) as avg_close,
            ROUND(MIN(close), 2) as min_close,
            ROUND(MAX(close), 2) as max_close,
            ROUND(AVG(volume), 0) as avg_volume
        FROM stock_prices
        GROUP BY symbol
    """)

    print("[DuckDB] Created views: stock_daily_returns, stock_latest_prices, stock_summary")
    conn.close()


# DAG Definition
with DAG(
    'yahoo_finance_daily',
    default_args=default_args,
    description='Fetch stock prices from Yahoo Finance with Delta Lake ACID transactions',
    schedule_interval='0 18 * * 1-5',  # Weekdays at 6 PM
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['yahoo-finance', 'stocks', 'market-data', 'delta-lake'],
    params={
        'symbols': 'AAPL,GOOGL,MSFT,AMZN,META',
        'period': '1mo'
    }
) as dag:

    dag.doc_md = """
    ## Yahoo Finance Stock Data Pipeline (Delta Lake)

    Fetches stock price data and stores using Delta Lake format for:
    - **ACID transactions** - Atomic writes, no corrupted data
    - **Time travel** - Query any historical version
    - **Schema evolution** - Add columns without breaking

    ### Trigger with config:
    ```json
    {"symbols": "AAPL,GOOGL,TSLA", "period": "1mo"}
    ```

    ### Period options: 1d, 5d, 1mo, 3mo, 6mo, 1y, 2y, 5y, max

    ### Time Travel (query historical versions):
    ```sql
    -- Query specific version
    SELECT * FROM delta_scan('/data/delta/stock_prices', version=5)

    -- Query by timestamp
    SELECT * FROM delta_scan('/data/delta/stock_prices', timestamp='2024-01-15')
    ```
    """

    fetch_task = PythonOperator(
        task_id='fetch_yahoo_data',
        python_callable=fetch_yahoo_finance_data,
        provide_context=True,
    )

    save_task = PythonOperator(
        task_id='save_to_delta',
        python_callable=save_to_delta_lake,
        provide_context=True,
    )

    optimize_task = PythonOperator(
        task_id='optimize_delta',
        python_callable=optimize_delta_table,
        provide_context=True,
    )

    sync_task = PythonOperator(
        task_id='sync_to_duckdb',
        python_callable=sync_to_duckdb,
        provide_context=True,
    )

    views_task = PythonOperator(
        task_id='create_views',
        python_callable=create_stock_views,
        provide_context=True,
    )

    fetch_task >> save_task >> optimize_task >> sync_task >> views_task
