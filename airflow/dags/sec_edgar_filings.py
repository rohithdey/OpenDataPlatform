"""
SEC EDGAR Filings Pipeline DAG
Fetches company filings from the SEC's EDGAR database.

Completely free - no API key required.
Data includes 10-K, 10-Q, 8-K, and other SEC filings.

SEC EDGAR API Documentation:
https://www.sec.gov/edgar/sec-api-documentation
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os
import pandas as pd

# Configuration
DATA_DIR = '/opt/airflow/data'
DUCKDB_PATH = '/opt/airflow/data/warehouse.duckdb'
DELTA_TABLE_PATH = '/opt/airflow/data/delta/sec_filings'

# Default companies to track (CIK numbers)
DEFAULT_COMPANIES = {
    'AAPL': '0000320193',
    'MSFT': '0000789019',
    'GOOGL': '0001652044',
    'AMZN': '0001018724',
    'META': '0001326801',
    'TSLA': '0001318605',
    'NVDA': '0001045810',
    'JPM': '0000019617',
    'V': '0001403161',
    'JNJ': '0000200406',
}

# Filing types to fetch
FILING_TYPES = ['10-K', '10-Q', '8-K', 'DEF 14A', '4']

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}


def fetch_sec_filings(**context):
    """
    Fetch recent filings from SEC EDGAR.
    Uses SEC's free API - requires User-Agent header.
    """
    import requests
    import time

    dag_run = context.get('dag_run')
    conf = dag_run.conf if dag_run and dag_run.conf else {}

    companies = conf.get('companies', DEFAULT_COMPANIES)
    filing_types = conf.get('filing_types', FILING_TYPES)

    if isinstance(companies, str):
        # Parse "AAPL:0000320193,MSFT:0000789019" format
        companies = dict(item.split(':') for item in companies.split(','))

    print(f"[SEC EDGAR] Fetching filings for: {list(companies.keys())}")

    all_filings = []

    # SEC requires User-Agent header
    headers = {
        'User-Agent': 'OpenDataPlatform/1.0 (contact@example.com)',
        'Accept': 'application/json'
    }

    for ticker, cik in companies.items():
        try:
            print(f"[SEC EDGAR] Fetching filings for {ticker} (CIK: {cik})...")

            # SEC submissions endpoint
            url = f"https://data.sec.gov/submissions/CIK{cik.zfill(10)}.json"

            response = requests.get(url, headers=headers, timeout=30)

            if response.status_code == 200:
                data = response.json()

                # Get company info
                company_name = data.get('name', ticker)

                # Get recent filings
                recent = data.get('filings', {}).get('recent', {})

                if recent:
                    forms = recent.get('form', [])
                    dates = recent.get('filingDate', [])
                    accessions = recent.get('accessionNumber', [])
                    descriptions = recent.get('primaryDocument', [])

                    for i in range(min(len(forms), 100)):  # Limit to 100 most recent
                        form_type = forms[i] if i < len(forms) else ''

                        # Filter by filing type if specified
                        if filing_types and form_type not in filing_types:
                            continue

                        filing = {
                            'ticker': ticker,
                            'cik': cik,
                            'company_name': company_name,
                            'form_type': form_type,
                            'filing_date': dates[i] if i < len(dates) else '',
                            'accession_number': accessions[i] if i < len(accessions) else '',
                            'document': descriptions[i] if i < len(descriptions) else '',
                        }

                        # Build filing URL
                        if filing['accession_number']:
                            acc_no_clean = filing['accession_number'].replace('-', '')
                            filing['filing_url'] = f"https://www.sec.gov/Archives/edgar/data/{cik}/{acc_no_clean}/{filing['document']}"

                        all_filings.append(filing)

                    print(f"[SEC EDGAR] Got {len([f for f in all_filings if f['ticker'] == ticker])} filings for {ticker}")

            elif response.status_code == 429:
                print(f"[SEC EDGAR] Rate limited, waiting...")
                time.sleep(10)
            else:
                print(f"[SEC EDGAR] Error for {ticker}: {response.status_code}")

            time.sleep(0.5)  # Rate limiting (SEC allows 10 requests/second)

        except Exception as e:
            print(f"[SEC EDGAR] Error fetching {ticker}: {e}")

    if not all_filings:
        raise ValueError("No filings fetched from SEC EDGAR")

    df = pd.DataFrame(all_filings)
    df['fetch_timestamp'] = datetime.now().isoformat()

    print(f"[SEC EDGAR] Total filings fetched: {len(df)}")

    return df.to_json(orient='records', date_format='iso')


def save_to_delta_lake(**context):
    """Save SEC filings to Delta Lake."""
    from deltalake import write_deltalake, DeltaTable
    import pyarrow as pa
    from io import StringIO

    ti = context['ti']
    json_data = ti.xcom_pull(task_ids='fetch_sec_filings')

    if not json_data:
        raise ValueError("No data received from fetch task")

    # Fix FutureWarning by wrapping in StringIO
    df = pd.read_json(StringIO(json_data), orient='records')
    print(f"[Delta Lake] Processing {len(df)} filings")

    os.makedirs(os.path.dirname(DELTA_TABLE_PATH), exist_ok=True)

    df['ingestion_date'] = datetime.now().date()
    if 'filing_date' in df.columns:
        df['filing_date'] = pd.to_datetime(df['filing_date'])

    # Convert to PyArrow Table for Delta Lake compatibility
    table = pa.Table.from_pandas(df)

    try:
        delta_log_path = os.path.join(DELTA_TABLE_PATH, '_delta_log')
        if os.path.exists(DELTA_TABLE_PATH) and os.path.exists(delta_log_path):
            write_deltalake(DELTA_TABLE_PATH, table, mode="append", schema_mode="merge")
        else:
            write_deltalake(DELTA_TABLE_PATH, table, mode="overwrite", partition_by=["ticker"])

        dt = DeltaTable(DELTA_TABLE_PATH)
        print(f"[Delta Lake] Saved. Version: {dt.version()}")

        return {'path': DELTA_TABLE_PATH, 'version': dt.version(), 'records': len(df)}

    except Exception as e:
        print(f"[Delta Lake] Error: {e}")
        raise


def sync_to_duckdb(**context):
    """Sync to DuckDB."""
    import duckdb

    ti = context['ti']
    delta_info = ti.xcom_pull(task_ids='save_to_delta')

    if not delta_info:
        return

    conn = duckdb.connect(DUCKDB_PATH, read_only=False)

    try:
        conn.execute("INSTALL delta;")
        conn.execute("LOAD delta;")
        conn.execute(f"""
            CREATE OR REPLACE TABLE sec_filings AS
            SELECT * FROM delta_scan('{DELTA_TABLE_PATH}')
        """)
    except:
        conn.execute(f"""
            CREATE OR REPLACE TABLE sec_filings AS
            SELECT * FROM read_parquet('{DELTA_TABLE_PATH}/*.parquet')
        """)

    count = conn.execute("SELECT COUNT(*) FROM sec_filings").fetchone()[0]
    print(f"[DuckDB] Synced {count} filings")

    conn.close()


def create_filing_views(**context):
    """Create useful views for SEC filings analysis."""
    import duckdb

    conn = duckdb.connect(DUCKDB_PATH, read_only=False)

    # Recent 10-K and 10-Q filings
    conn.execute("""
        CREATE OR REPLACE VIEW sec_annual_quarterly AS
        SELECT
            ticker,
            company_name,
            form_type,
            filing_date,
            filing_url
        FROM sec_filings
        WHERE form_type IN ('10-K', '10-Q')
        ORDER BY filing_date DESC
    """)

    # Material events (8-K filings)
    conn.execute("""
        CREATE OR REPLACE VIEW sec_material_events AS
        SELECT
            ticker,
            company_name,
            filing_date,
            accession_number,
            filing_url
        FROM sec_filings
        WHERE form_type = '8-K'
        ORDER BY filing_date DESC
    """)

    # Insider transactions (Form 4)
    conn.execute("""
        CREATE OR REPLACE VIEW sec_insider_trades AS
        SELECT
            ticker,
            company_name,
            filing_date,
            filing_url
        FROM sec_filings
        WHERE form_type = '4'
        ORDER BY filing_date DESC
    """)

    # Filing counts by company
    conn.execute("""
        CREATE OR REPLACE VIEW sec_filing_summary AS
        SELECT
            ticker,
            company_name,
            form_type,
            COUNT(*) as filing_count,
            MAX(filing_date) as latest_filing
        FROM sec_filings
        GROUP BY ticker, company_name, form_type
        ORDER BY ticker, form_type
    """)

    print("[DuckDB] Created views: sec_annual_quarterly, sec_material_events, sec_insider_trades, sec_filing_summary")
    conn.close()


# DAG Definition
with DAG(
    'sec_edgar_daily',
    default_args=default_args,
    description='Fetch company filings from SEC EDGAR',
    schedule_interval='0 7 * * 1-5',  # Weekdays at 7 AM
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['sec', 'edgar', 'filings', 'regulatory', 'delta-lake'],
) as dag:

    dag.doc_md = """
    ## SEC EDGAR Filings Pipeline

    Fetches company filings from SEC's EDGAR database.

    ### Default Companies:
    AAPL, MSFT, GOOGL, AMZN, META, TSLA, NVDA, JPM, V, JNJ

    ### Filing Types:
    - **10-K** - Annual reports
    - **10-Q** - Quarterly reports
    - **8-K** - Material events
    - **DEF 14A** - Proxy statements
    - **4** - Insider transactions

    ### Trigger with custom companies:
    ```json
    {
        "companies": {"AAPL": "0000320193", "TSLA": "0001318605"},
        "filing_types": ["10-K", "10-Q", "8-K"]
    }
    ```

    ### Find CIK numbers:
    https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany
    """

    fetch_task = PythonOperator(
        task_id='fetch_sec_filings',
        python_callable=fetch_sec_filings,
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
        python_callable=create_filing_views,
        provide_context=True,
    )

    fetch_task >> save_task >> sync_task >> views_task
