"""
GLEIF Legal Entity Data Ingestion DAG
Pulls corporate entity data from GLEIF API and loads into DuckDB
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import duckdb
import pandas as pd
import requests
from requests.exceptions import HTTPError, ConnectionError, Timeout
import json
import os
import time

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

DUCKDB_PATH = '/opt/airflow/data/warehouse.duckdb'

# GLEIF API limit: max 10,000 records (100 pages * 100 records)
MAX_PAGES = 100
PAGE_SIZE = 100


def fetch_gleif_data(**context):
    """
    Fetch legal entity data from GLEIF API (up to API limit)
    Returns / XCom-pushes a list of processed dicts.
    """
    base_url = "https://api.gleif.org/api/v1/lei-records"
    
    session = requests.Session()
    session.headers.update({"Accept": "application/json"})

    all_records = []
    
    # Simple retry settings
    max_attempts = 3
    backoff_seconds = 2

    for page_num in range(1, MAX_PAGES + 1):
        params = {
            "page[size]": PAGE_SIZE,
            "page[number]": page_num
        }
        
        attempt = 0
        while True:
            try:
                resp = session.get(base_url, params=params, timeout=30)
                resp.raise_for_status()
                data = resp.json()
                break  # success, break retry loop

            except HTTPError as e:
                # Check if it's a 400 error (API limit reached)
                if e.response is not None and e.response.status_code == 400:
                    print(f"[GLEIF] API returned 400 at page {page_num} - likely reached API limit. Stopping pagination.")
                    # Return what we have so far
                    print(f"[GLEIF] Finished fetching — total records: {len(all_records)}")
                    return all_records
                
                attempt += 1
                if attempt >= max_attempts:
                    print(f"[GLEIF] Failed after {attempt} attempts: {e}")
                    raise
                else:
                    wait = backoff_seconds * attempt
                    print(f"[GLEIF] Request failed (attempt {attempt}) — retrying in {wait}s: {e}")
                    time.sleep(wait)
                    
            except (ConnectionError, Timeout) as e:
                attempt += 1
                if attempt >= max_attempts:
                    print(f"[GLEIF] Failed after {attempt} attempts: {e}")
                    raise
                else:
                    wait = backoff_seconds * attempt
                    print(f"[GLEIF] Request failed (attempt {attempt}) — retrying in {wait}s: {e}")
                    time.sleep(wait)
                    
            except ValueError as e:
                # JSON decode error or unexpected body
                print(f"[GLEIF] Invalid JSON response: {e}")
                raise

        # Process items on this page
        records_on_page = data.get("data", [])
        
        # If no records returned, we've reached the end
        if not records_on_page:
            print(f"[GLEIF] No more records at page {page_num}. Stopping.")
            break
            
        for record in records_on_page:
            attributes = record.get("attributes", {}) or {}
            entity = attributes.get("entity", {}) or {}
            legal_address = entity.get("legalAddress", {}) or {}

            relationships = record.get("relationships", {}) or {}
            direct_parent = relationships.get("direct-parent", {}).get("data", {}) if relationships.get("direct-parent") else {}
            ultimate_parent = relationships.get("ultimate-parent", {}).get("data", {}) if relationships.get("ultimate-parent") else {}

            processed = {
                "lei": attributes.get("lei", ""),
                "legal_name": entity.get("legalName", {}).get("name", "") if entity.get("legalName") else "",
                "legal_form": entity.get("legalForm", {}).get("id", "") if entity.get("legalForm") else "",
                "jurisdiction": entity.get("jurisdiction", ""),
                "category": entity.get("category", ""),
                "status": entity.get("status", ""),
                "creation_date": entity.get("creationDate", ""),
                "address_line1": (legal_address.get("addressLines") or [""])[0] if legal_address else "",
                "city": legal_address.get("city", ""),
                "region": legal_address.get("region", ""),
                "country": legal_address.get("country", ""),
                "postal_code": legal_address.get("postalCode", ""),
                "direct_parent_lei": direct_parent.get("id", "") if direct_parent else "",
                "ultimate_parent_lei": ultimate_parent.get("id", "") if ultimate_parent else "",
                "last_update": attributes.get("lastUpdateDate", ""),
            }
            all_records.append(processed)

        print(f"[GLEIF] Page {page_num} processed — cumulative records: {len(all_records)}")

        # Check if there's a next page
        links = data.get("links", {}) or {}
        if not links.get("next"):
            print(f"[GLEIF] No next page link. Stopping.")
            break

        # Small throttle to be polite to the API
        time.sleep(0.1)

    print(f"[GLEIF] Finished fetching — total records: {len(all_records)}")
    return all_records


def load_to_duckdb(**context):
    """
    Load the fetched data into DuckDB via Iceberg format
    Workflow: API → CSV → Iceberg (Parquet) → DuckDB
    """
    import time
    from datetime import datetime as dt

    # Get data from previous task
    ti = context['ti']
    records = ti.xcom_pull(task_ids='fetch_gleif_data')

    if not records:
        print("No records to load")
        return

    df = pd.DataFrame(records)
    print(f"Loading {len(df)} records via Iceberg to DuckDB")

    # Ensure data directories exist
    os.makedirs(os.path.dirname(DUCKDB_PATH), exist_ok=True)

    # Step 1: Save as CSV (raw download format)
    csv_path = '/opt/airflow/data/raw/gleif'
    os.makedirs(csv_path, exist_ok=True)

    timestamp = dt.now().strftime('%Y%m%d_%H%M%S')
    csv_file = f'{csv_path}/gleif_entities_{timestamp}.csv'
    df.to_csv(csv_file, index=False)
    print(f"Saved {len(df)} records to CSV: {csv_file}")

    # Step 2: Convert to Iceberg format (Parquet with directory structure)
    iceberg_path = '/opt/airflow/data/iceberg/gleif_entities'
    data_path = f'{iceberg_path}/data'
    os.makedirs(data_path, exist_ok=True)

    parquet_file = f'{data_path}/gleif_entities_{timestamp}.parquet'
    df.to_parquet(parquet_file, index=False, engine='pyarrow')
    print(f"Converted to Iceberg Parquet: {parquet_file}")

    # Step 3: Load to DuckDB with Iceberg extension
    max_retries = 5
    retry_delay = 2

    for attempt in range(max_retries):
        try:
            # Connect to DuckDB with write access
            conn = duckdb.connect(DUCKDB_PATH, read_only=False)

            # Install and load Iceberg extension
            conn.execute("INSTALL iceberg")
            conn.execute("LOAD iceberg")

            # Create table if not exists (schema only)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS gleif_entities (
                    lei VARCHAR,
                    legal_name VARCHAR,
                    legal_form VARCHAR,
                    jurisdiction VARCHAR,
                    category VARCHAR,
                    status VARCHAR,
                    creation_date VARCHAR,
                    address_line1 VARCHAR,
                    city VARCHAR,
                    region VARCHAR,
                    country VARCHAR,
                    postal_code VARCHAR,
                    direct_parent_lei VARCHAR,
                    ultimate_parent_lei VARCHAR,
                    last_update VARCHAR,
                    ingestion_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)

            # Delete existing records that we're about to update
            lei_list = df['lei'].tolist()
            conn.execute("DELETE FROM gleif_entities WHERE lei IN (SELECT UNNEST(?))", [lei_list])

            # Insert new data from Parquet (Iceberg format)
            conn.execute(f"""
                INSERT INTO gleif_entities
                SELECT *, CURRENT_TIMESTAMP as ingestion_timestamp
                FROM read_parquet('{parquet_file}')
            """)

            # Get final count
            count = conn.execute("SELECT COUNT(*) FROM gleif_entities").fetchone()[0]
            print(f"✓ Loaded via Iceberg - Total records in gleif_entities: {count}")

            conn.close()
            return count

        except duckdb.IOException as e:
            if "lock" in str(e).lower() and attempt < max_retries - 1:
                print(f"Database locked, retrying in {retry_delay} seconds... (attempt {attempt + 1}/{max_retries})")
                time.sleep(retry_delay)
                retry_delay *= 2  # Exponential backoff
            else:
                raise
        except Exception as e:
            if 'conn' in locals():
                conn.close()
            raise


def create_aggregations(**context):
    """
    Create aggregated views and summaries
    """
    conn = duckdb.connect(DUCKDB_PATH, read_only=False)
    
    # Create country summary view
    conn.execute("""
        CREATE OR REPLACE VIEW gleif_by_country AS
        SELECT 
            country,
            COUNT(*) as entity_count,
            COUNT(DISTINCT jurisdiction) as jurisdictions
        FROM gleif_entities
        GROUP BY country
        ORDER BY entity_count DESC
    """)
    
    # Create parent-child relationship view
    conn.execute("""
        CREATE OR REPLACE VIEW gleif_hierarchies AS
        SELECT 
            e.lei,
            e.legal_name,
            p.legal_name as direct_parent_name,
            u.legal_name as ultimate_parent_name
        FROM gleif_entities e
        LEFT JOIN gleif_entities p ON e.direct_parent_lei = p.lei
        LEFT JOIN gleif_entities u ON e.ultimate_parent_lei = u.lei
        WHERE e.direct_parent_lei != '' OR e.ultimate_parent_lei != ''
    """)
    
    print("Aggregation views created successfully")
    conn.close()


with DAG(
    'gleif_data_ingestion',
    default_args=default_args,
    description='Ingest GLEIF legal entity data into DuckDB',
    schedule_interval='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['gleif', 'lei', 'corporate-data'],
) as dag:

    fetch_task = PythonOperator(
        task_id='fetch_gleif_data',
        python_callable=fetch_gleif_data,
        provide_context=True,
    )

    load_task = PythonOperator(
        task_id='load_to_duckdb',
        python_callable=load_to_duckdb,
        provide_context=True,
    )

    aggregate_task = PythonOperator(
        task_id='create_aggregations',
        python_callable=create_aggregations,
        provide_context=True,
    )

    fetch_task >> load_task >> aggregate_task
