"""
GLEIF Golden Copy Data Ingestion DAG
Downloads the full LEI dataset from GLEIF Golden Copy files and loads into DuckDB

Memory-efficient: Uses chunked loading to handle the ~2.5M record dataset
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import duckdb
import pandas as pd
import requests
import os
import zipfile

# =============================================================================
# CONFIGURATION - Single source of truth
# =============================================================================
DATA_DIR = '/opt/airflow/data'
DUCKDB_PATH = '/opt/airflow/data/warehouse.duckdb'  # Must match API path!
CSV_PATH = '/opt/airflow/data/gleif_golden_copy.csv'
ZIP_PATH = '/opt/airflow/data/gleif_golden_copy.zip'

# GLEIF Golden Copy URLs - try multiple in case one fails
GOLDEN_COPY_URLS = [
    "https://leidata.gleif.org/api/v1/concatenated-files/lei2/get/30447/zip",
    "https://goldencopy.gleif.org/api/v2/golden-copies/publishes/lei2/latest",
    "https://leidata-preview.gleif.org/storage/golden-copy/2024/11/01/lei2/20241101-gleif-goldencopy-lei2-golden-copy.csv.zip",
]

# Chunking config - tune based on available memory
CHUNK_SIZE = 100_000  # rows per chunk

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}


# =============================================================================
# TASK 1: Download Golden Copy
# =============================================================================
def download_golden_copy(**context):
    """
    Download the GLEIF Golden Copy ZIP and extract CSV.
    Uses streaming to handle large file without loading into memory.
    Tries multiple URLs in case one fails.
    """
    os.makedirs(DATA_DIR, exist_ok=True)

    # Clean up any existing files
    for path in [ZIP_PATH, CSV_PATH]:
        if os.path.exists(path):
            os.remove(path)
            print(f"[GLEIF] Removed existing: {path}")

    # Try each URL until one works
    session = requests.Session()
    session.headers.update({
        'User-Agent': 'Mozilla/5.0 (compatible; OpenDataPlatform/1.0)',
        'Accept': '*/*'
    })

    response = None
    working_url = None

    for url in GOLDEN_COPY_URLS:
        print(f"[GLEIF] Trying URL: {url}")
        try:
            response = session.get(url, stream=True, timeout=600, allow_redirects=True)
            if response.status_code == 200:
                working_url = url
                print(f"[GLEIF] Success! Using: {response.url}")
                break
            else:
                print(f"[GLEIF] Failed with status {response.status_code}")
        except Exception as e:
            print(f"[GLEIF] Failed: {e}")

    if not response or response.status_code != 200:
        raise Exception(f"All GLEIF Golden Copy URLs failed. Tried: {GOLDEN_COPY_URLS}")
    
    print(f"[GLEIF] Downloading from: {response.url}")
    
    # Stream to disk in chunks
    total_bytes = 0
    with open(ZIP_PATH, 'wb') as f:
        for chunk in response.iter_content(chunk_size=1024 * 1024):  # 1MB chunks
            if chunk:
                f.write(chunk)
                total_bytes += len(chunk)
                if total_bytes % (50 * 1024 * 1024) == 0:  # Log every 50MB
                    print(f"[GLEIF] Downloaded: {total_bytes / (1024*1024):.0f} MB")
    
    print(f"[GLEIF] Download complete: {total_bytes / (1024*1024):.1f} MB")
    
    # Extract CSV from ZIP
    print("[GLEIF] Extracting CSV from ZIP...")
    with zipfile.ZipFile(ZIP_PATH, 'r') as zf:
        csv_files = [f for f in zf.namelist() if f.endswith('.csv')]
        if not csv_files:
            raise Exception("No CSV file found in ZIP")
        
        csv_name = csv_files[0]
        print(f"[GLEIF] Extracting: {csv_name}")
        
        # Extract and rename
        zf.extract(csv_name, DATA_DIR)
        extracted = os.path.join(DATA_DIR, csv_name)
        os.rename(extracted, CSV_PATH)
    
    # Clean up ZIP
    os.remove(ZIP_PATH)
    
    # Count rows (fast line count)
    with open(CSV_PATH, 'r', encoding='utf-8', errors='ignore') as f:
        row_count = sum(1 for _ in f) - 1
    
    print(f"[GLEIF] CSV ready: {row_count:,} records")
    return CSV_PATH


# =============================================================================
# TASK 2: Load to DuckDB (Memory-Efficient Chunked Approach)
# =============================================================================
def load_to_duckdb(**context):
    """
    Load CSV into DuckDB using chunked pandas reading.
    This avoids loading the entire file into memory at once.
    """
    ti = context['ti']
    csv_path = ti.xcom_pull(task_ids='download_golden_copy')
    
    if not csv_path or not os.path.exists(csv_path):
        raise Exception(f"CSV not found: {csv_path}")
    
    print(f"[GLEIF] Loading CSV to DuckDB: {csv_path}")
    print(f"[GLEIF] Target database: {DUCKDB_PATH}")
    
    # Connect to DuckDB
    conn = duckdb.connect(DUCKDB_PATH, read_only=False)
    
    # Drop existing table
    conn.execute("DROP TABLE IF EXISTS gleif_entities_full")
    print("[GLEIF] Dropped existing table")
    
    # Process in chunks
    total_rows = 0
    chunk_num = 0
    table_created = False
    
    # Read CSV in chunks - all columns as strings to avoid type issues
    chunks = pd.read_csv(
        csv_path,
        chunksize=CHUNK_SIZE,
        dtype=str,  # Read everything as string - simple and safe
        low_memory=True,
        on_bad_lines='skip'  # Skip malformed rows
    )
    
    for chunk in chunks:
        chunk_num += 1
        chunk_rows = len(chunk)
        
        # Replace NaN with empty string
        chunk = chunk.fillna('')
        
        # Register chunk as a view
        conn.register('chunk_data', chunk)
        
        if not table_created:
            # Create table from first chunk
            conn.execute("CREATE TABLE gleif_entities_full AS SELECT * FROM chunk_data")
            table_created = True
            print(f"[GLEIF] Created table from chunk 1 ({chunk_rows:,} rows)")
        else:
            # Append subsequent chunks
            conn.execute("INSERT INTO gleif_entities_full SELECT * FROM chunk_data")
            print(f"[GLEIF] Chunk {chunk_num}: +{chunk_rows:,} rows")
        
        # Unregister to free memory
        conn.unregister('chunk_data')
        total_rows += chunk_rows
    
    # Verify
    final_count = conn.execute("SELECT COUNT(*) FROM gleif_entities_full").fetchone()[0]
    print(f"[GLEIF] Load complete: {final_count:,} rows in gleif_entities_full")
    
    conn.close()
    return final_count


# =============================================================================
# TASK 3: Create Views
# =============================================================================
def create_views(**context):
    """
    Create useful views for querying the data.
    """
    conn = duckdb.connect(DUCKDB_PATH, read_only=False)
    
    # Get column names
    cols = conn.execute("""
        SELECT column_name 
        FROM information_schema.columns 
        WHERE table_name = 'gleif_entities_full'
    """).fetchall()
    col_names = [c[0] for c in cols]
    print(f"[GLEIF] Table has {len(col_names)} columns")
    print(f"[GLEIF] Sample columns: {col_names[:10]}")
    
    # Create country summary view
    # The column name in Golden Copy CSV is typically "Entity.LegalAddress.Country"
    country_col = None
    for possible in ['Entity.LegalAddress.Country', 'LegalAddress.Country', 'Country']:
        if possible in col_names:
            country_col = possible
            break
    
    if country_col:
        conn.execute(f"""
            CREATE OR REPLACE VIEW gleif_by_country AS
            SELECT 
                "{country_col}" as country,
                COUNT(*) as entity_count
            FROM gleif_entities_full
            WHERE "{country_col}" IS NOT NULL AND "{country_col}" != ''
            GROUP BY "{country_col}"
            ORDER BY entity_count DESC
        """)
        print(f"[GLEIF] Created view: gleif_by_country")
    
    # Create a simplified entities view with key columns
    # Find the relevant columns
    lei_col = next((c for c in col_names if 'LEI' in c.upper() and 'PARENT' not in c.upper()), None)
    name_col = next((c for c in col_names if 'LegalName' in c or 'Legal.Name' in c), None)
    
    print(f"[GLEIF] LEI column: {lei_col}")
    print(f"[GLEIF] Name column: {name_col}")
    
    # Get summary stats
    total = conn.execute("SELECT COUNT(*) FROM gleif_entities_full").fetchone()[0]
    
    if country_col:
        countries = conn.execute(f"""
            SELECT COUNT(DISTINCT "{country_col}") 
            FROM gleif_entities_full 
            WHERE "{country_col}" != ''
        """).fetchone()[0]
    else:
        countries = 0
    
    print(f"[GLEIF] Summary: {total:,} entities across {countries} countries")
    
    conn.close()
    return {"total": total, "countries": countries}


# =============================================================================
# DAG Definition
# =============================================================================
with DAG(
    'gleif_golden_copy_ingestion',
    default_args=default_args,
    description='Download full GLEIF Golden Copy dataset (~2.5M records)',
    schedule_interval='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['gleif', 'lei', 'golden-copy', 'bulk-data'],
) as dag:
    
    dag.doc_md = """
    ## GLEIF Golden Copy Ingestion
    
    Downloads the complete GLEIF LEI dataset (~2.5M records) and loads into DuckDB.
    
    ### Tables Created
    - `gleif_entities_full` - All LEI records with all columns
    - `gleif_by_country` - View: entity count by country
    
    ### Memory Management
    Uses chunked loading (100K rows at a time) to avoid memory issues.
    
    ### Runtime
    - Download: 5-10 minutes (~500MB compressed)
    - Load: 5-10 minutes
    - Total: ~15-20 minutes first run
    """

    t1 = PythonOperator(
        task_id='download_golden_copy',
        python_callable=download_golden_copy,
    )

    t2 = PythonOperator(
        task_id='load_to_duckdb',
        python_callable=load_to_duckdb,
    )

    t3 = PythonOperator(
        task_id='create_views',
        python_callable=create_views,
    )

    t1 >> t2 >> t3