"""
FRED (Federal Reserve Economic Data) Pipeline DAG
Fetches economic indicators from the Federal Reserve Bank of St. Louis.

Free data source - no API key required for basic access.
Full API access available with free registration at https://fred.stlouisfed.org/

Data includes:
- GDP, unemployment, inflation (CPI)
- Interest rates (Fed Funds, Treasury yields)
- Housing data, consumer sentiment
- And 800,000+ other economic time series
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os
import pandas as pd

# Configuration
DATA_DIR = '/opt/airflow/data'
DUCKDB_PATH = '/opt/airflow/data/warehouse.duckdb'
DELTA_TABLE_PATH = '/opt/airflow/data/delta/fred_economic'

# Popular FRED series IDs
DEFAULT_SERIES = [
    'GDP',           # Gross Domestic Product
    'UNRATE',        # Unemployment Rate
    'CPIAUCSL',      # Consumer Price Index (All Urban Consumers)
    'FEDFUNDS',      # Federal Funds Rate
    'DGS10',         # 10-Year Treasury Yield
    'DGS2',          # 2-Year Treasury Yield
    'MORTGAGE30US',  # 30-Year Mortgage Rate
    'HOUST',         # Housing Starts
    'UMCSENT',       # Consumer Sentiment
    'INDPRO',        # Industrial Production Index
]

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}


def fetch_fred_data(**context):
    """
    Fetch economic data from FRED using their public API.
    Works without API key for basic access (limited requests).
    """
    import requests
    import time

    dag_run = context.get('dag_run')
    conf = dag_run.conf if dag_run and dag_run.conf else {}

    series_list = conf.get('series', DEFAULT_SERIES)
    api_key = conf.get('api_key', os.environ.get('FRED_API_KEY', ''))

    if isinstance(series_list, str):
        series_list = [s.strip().upper() for s in series_list.split(',')]

    print(f"[FRED] Fetching series: {series_list}")

    all_data = []
    base_url = "https://api.stlouisfed.org/fred/series/observations"

    for series_id in series_list:
        try:
            print(f"[FRED] Fetching {series_id}...")

            params = {
                'series_id': series_id,
                'file_type': 'json',
                'sort_order': 'desc',
                'limit': 1000,
            }

            if api_key:
                params['api_key'] = api_key

            response = requests.get(base_url, params=params, timeout=30)

            if response.status_code == 200:
                data = response.json()

                if 'observations' in data:
                    observations = data['observations']

                    for obs in observations:
                        if obs['value'] != '.':  # FRED uses '.' for missing values
                            all_data.append({
                                'series_id': series_id,
                                'date': obs['date'],
                                'value': float(obs['value']),
                                'realtime_start': obs.get('realtime_start', ''),
                                'realtime_end': obs.get('realtime_end', ''),
                            })

                    print(f"[FRED] Got {len(observations)} observations for {series_id}")
                else:
                    print(f"[FRED] No observations for {series_id}")

            elif response.status_code == 429:
                print(f"[FRED] Rate limited. Consider adding an API key.")
                time.sleep(60)
            else:
                print(f"[FRED] Error fetching {series_id}: {response.status_code}")

            time.sleep(1)  # Rate limiting

        except Exception as e:
            print(f"[FRED] Error fetching {series_id}: {e}")

    if not all_data:
        raise ValueError("No data fetched from FRED")

    df = pd.DataFrame(all_data)
    df['fetch_timestamp'] = datetime.now().isoformat()

    print(f"[FRED] Total records fetched: {len(df)}")
    print(f"[FRED] Series: {df['series_id'].unique().tolist()}")

    return df.to_json(orient='records', date_format='iso')


def save_to_delta_lake(**context):
    """Save FRED data to Delta Lake with ACID transactions."""
    from deltalake import write_deltalake, DeltaTable

    ti = context['ti']
    json_data = ti.xcom_pull(task_ids='fetch_fred_data')

    if not json_data:
        raise ValueError("No data received from fetch task")

    df = pd.read_json(json_data, orient='records')
    print(f"[Delta Lake] Processing {len(df)} records")

    os.makedirs(os.path.dirname(DELTA_TABLE_PATH), exist_ok=True)

    df['ingestion_date'] = datetime.now().date()
    df['date'] = pd.to_datetime(df['date'])

    try:
        delta_log_path = os.path.join(DELTA_TABLE_PATH, '_delta_log')
        if os.path.exists(DELTA_TABLE_PATH) and os.path.exists(delta_log_path):
            print("[Delta Lake] Appending to existing table...")
            write_deltalake(
                DELTA_TABLE_PATH,
                df,
                mode="append",
                schema_mode="merge"
            )
        else:
            print("[Delta Lake] Creating new Delta table...")
            write_deltalake(
                DELTA_TABLE_PATH,
                df,
                mode="overwrite",
                partition_by=["series_id"]
            )

        dt = DeltaTable(DELTA_TABLE_PATH)
        print(f"[Delta Lake] Saved. Version: {dt.version()}")

        return {'path': DELTA_TABLE_PATH, 'version': dt.version(), 'records': len(df)}

    except Exception as e:
        print(f"[Delta Lake] Error: {e}")
        raise


def sync_to_duckdb(**context):
    """Sync Delta Lake table to DuckDB."""
    import duckdb

    ti = context['ti']
    delta_info = ti.xcom_pull(task_ids='save_to_delta')

    if not delta_info:
        return

    print(f"[DuckDB] Syncing FRED data from Delta Lake")

    conn = duckdb.connect(DUCKDB_PATH, read_only=False)

    try:
        conn.execute("INSTALL delta;")
        conn.execute("LOAD delta;")
        conn.execute(f"""
            CREATE OR REPLACE TABLE fred_economic AS
            SELECT * FROM delta_scan('{DELTA_TABLE_PATH}')
        """)
    except Exception as e:
        print(f"[DuckDB] Delta extension failed, using parquet: {e}")
        conn.execute(f"""
            CREATE OR REPLACE TABLE fred_economic AS
            SELECT * FROM read_parquet('{DELTA_TABLE_PATH}/*.parquet')
        """)

    count = conn.execute("SELECT COUNT(*) FROM fred_economic").fetchone()[0]
    print(f"[DuckDB] Synced {count} records")

    conn.close()
    return count


def create_economic_views(**context):
    """Create useful views for economic analysis."""
    import duckdb

    conn = duckdb.connect(DUCKDB_PATH, read_only=False)

    # Latest values for each series
    conn.execute("""
        CREATE OR REPLACE VIEW fred_latest AS
        SELECT
            series_id,
            date as latest_date,
            value as latest_value
        FROM fred_economic
        WHERE (series_id, date) IN (
            SELECT series_id, MAX(date)
            FROM fred_economic
            GROUP BY series_id
        )
    """)

    # Year-over-year change
    conn.execute("""
        CREATE OR REPLACE VIEW fred_yoy_change AS
        SELECT
            a.series_id,
            a.date,
            a.value as current_value,
            b.value as year_ago_value,
            ROUND((a.value - b.value) / NULLIF(b.value, 0) * 100, 2) as yoy_change_pct
        FROM fred_economic a
        LEFT JOIN fred_economic b
            ON a.series_id = b.series_id
            AND b.date = a.date - INTERVAL '1 year'
        WHERE b.value IS NOT NULL
        ORDER BY a.series_id, a.date DESC
    """)

    # Treasury yield spread (10Y - 2Y, indicator of recession)
    conn.execute("""
        CREATE OR REPLACE VIEW treasury_yield_spread AS
        SELECT
            a.date,
            a.value as yield_10y,
            b.value as yield_2y,
            ROUND(a.value - b.value, 2) as spread_10y_2y
        FROM fred_economic a
        JOIN fred_economic b
            ON a.date = b.date
            AND a.series_id = 'DGS10'
            AND b.series_id = 'DGS2'
        ORDER BY a.date DESC
    """)

    print("[DuckDB] Created views: fred_latest, fred_yoy_change, treasury_yield_spread")
    conn.close()


# DAG Definition
with DAG(
    'fred_economic_daily',
    default_args=default_args,
    description='Fetch economic indicators from FRED (Federal Reserve)',
    schedule_interval='0 9 * * 1-5',  # Weekdays at 9 AM
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['fred', 'economic', 'macro', 'delta-lake'],
    params={
        'series': ','.join(DEFAULT_SERIES),
    }
) as dag:

    dag.doc_md = """
    ## FRED Economic Data Pipeline

    Fetches economic indicators from the Federal Reserve Bank of St. Louis.

    ### Default Series:
    - **GDP** - Gross Domestic Product
    - **UNRATE** - Unemployment Rate
    - **CPIAUCSL** - Consumer Price Index
    - **FEDFUNDS** - Federal Funds Rate
    - **DGS10/DGS2** - Treasury Yields
    - **MORTGAGE30US** - 30-Year Mortgage Rate
    - **HOUST** - Housing Starts
    - **UMCSENT** - Consumer Sentiment

    ### Trigger with custom series:
    ```json
    {"series": "GDP,UNRATE,CPIAUCSL", "api_key": "optional_key"}
    ```

    ### Get a free API key:
    https://fred.stlouisfed.org/docs/api/api_key.html
    """

    fetch_task = PythonOperator(
        task_id='fetch_fred_data',
        python_callable=fetch_fred_data,
        provide_context=True,
    )

    save_task = PythonOperator(
        task_id='save_to_delta',
        python_callable=save_to_delta_lake,
        provide_context=True,
    )

    sync_task = PythonOperator(
        task_id='sync_to_duckdb',
        python_callable=sync_to_duckdb,
        provide_context=True,
    )

    views_task = PythonOperator(
        task_id='create_views',
        python_callable=create_economic_views,
        provide_context=True,
    )

    fetch_task >> save_task >> sync_task >> views_task
