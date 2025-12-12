#!/usr/bin/env python3
"""
Comprehensive Test Suite for Open Data Platform
================================================

This script tests ALL functionality and writes detailed logs.

USAGE:
------
1. Copy this to your host machine
2. Run: python3 test_all.py > test_results.log 2>&1
3. Share test_results.log with Claude for debugging

OR run inside API container:
    docker exec -it opendataplatform-api-1 python /app/test_all.py > /app/data/test_results.log 2>&1

Then share the contents of data/test_results.log
"""

import sys
import os
import json
import time
import traceback
from datetime import datetime

# Test results collector
RESULTS = {
    "timestamp": datetime.now().isoformat(),
    "tests": [],
    "summary": {"passed": 0, "failed": 0, "skipped": 0}
}

def log(msg, level="INFO"):
    """Print timestamped log message"""
    ts = datetime.now().strftime("%H:%M:%S")
    print(f"[{ts}] [{level}] {msg}")

def test(name, func):
    """Run a test and record results"""
    log(f"Testing: {name}")
    result = {"name": name, "status": "unknown", "details": "", "error": None}

    try:
        details = func()
        result["status"] = "PASSED"
        result["details"] = str(details) if details else "OK"
        RESULTS["summary"]["passed"] += 1
        log(f"  ✓ PASSED: {details if details else 'OK'}", "PASS")
    except Exception as e:
        result["status"] = "FAILED"
        result["error"] = str(e)
        result["traceback"] = traceback.format_exc()
        RESULTS["summary"]["failed"] += 1
        log(f"  ✗ FAILED: {e}", "FAIL")
        log(f"    Traceback: {traceback.format_exc()}", "DEBUG")

    RESULTS["tests"].append(result)
    return result["status"] == "PASSED"

def skip(name, reason):
    """Skip a test"""
    log(f"Skipping: {name} - {reason}", "SKIP")
    RESULTS["tests"].append({"name": name, "status": "SKIPPED", "reason": reason})
    RESULTS["summary"]["skipped"] += 1

# =============================================================================
# ENVIRONMENT DETECTION
# =============================================================================
print("=" * 70)
print("OPEN DATA PLATFORM - COMPREHENSIVE TEST SUITE")
print("=" * 70)
print(f"Timestamp: {datetime.now().isoformat()}")
print(f"Python: {sys.version}")
print(f"Working Directory: {os.getcwd()}")
print()

# Detect environment
IN_DOCKER = os.path.exists('/.dockerenv') or os.environ.get('DOCKER_CONTAINER')
API_URL = os.environ.get('API_URL', 'http://localhost:8000')
OLLAMA_URL = os.environ.get('OLLAMA_URL', 'http://ollama:11434')
AIRFLOW_URL = os.environ.get('AIRFLOW_API_URL', 'http://airflow-webserver:8080/api/v1')

# If running on host, use localhost
if not IN_DOCKER:
    API_URL = 'http://localhost:8000'
    OLLAMA_URL = 'http://localhost:11434'
    AIRFLOW_URL = 'http://localhost:8080/api/v1'

log(f"Environment: {'Docker Container' if IN_DOCKER else 'Host Machine'}")
log(f"API URL: {API_URL}")
log(f"Ollama URL: {OLLAMA_URL}")
log(f"Airflow URL: {AIRFLOW_URL}")
print()

# =============================================================================
# TEST 1: DEPENDENCIES
# =============================================================================
print("\n" + "=" * 70)
print("TEST GROUP 1: DEPENDENCIES")
print("=" * 70)

def test_requests():
    import requests
    return f"requests {requests.__version__}"

def test_pandas():
    import pandas as pd
    return f"pandas {pd.__version__}"

def test_duckdb():
    import duckdb
    return f"duckdb {duckdb.__version__}"

def test_yfinance():
    import yfinance as yf
    return f"yfinance {yf.__version__}"

def test_pyarrow():
    import pyarrow
    return f"pyarrow {pyarrow.__version__}"

test("Import requests", test_requests)
test("Import pandas", test_pandas)
test("Import duckdb", test_duckdb)
test("Import yfinance", test_yfinance)
test("Import pyarrow", test_pyarrow)

# =============================================================================
# TEST 2: API CONNECTIVITY
# =============================================================================
print("\n" + "=" * 70)
print("TEST GROUP 2: API CONNECTIVITY")
print("=" * 70)

import requests

def test_api_health():
    r = requests.get(f"{API_URL}/", timeout=10)
    return f"Status {r.status_code}"

def test_api_docs():
    r = requests.get(f"{API_URL}/docs", timeout=10)
    if r.status_code == 200:
        return "Swagger UI accessible"
    raise Exception(f"Status {r.status_code}")

def test_api_tables():
    r = requests.get(f"{API_URL}/tables", timeout=10)
    if r.status_code == 200:
        data = r.json()
        return f"Found {len(data.get('tables', []))} tables: {data.get('tables', [])}"
    raise Exception(f"Status {r.status_code}: {r.text}")

test("API Health Check", test_api_health)
test("API Docs (Swagger)", test_api_docs)
test("API List Tables", test_api_tables)

# =============================================================================
# TEST 3: YAHOO FINANCE
# =============================================================================
print("\n" + "=" * 70)
print("TEST GROUP 3: YAHOO FINANCE")
print("=" * 70)

def test_yf_download_single():
    import yfinance as yf
    df = yf.download("AAPL", period="5d", progress=False)
    if df.empty:
        raise Exception("Empty DataFrame returned")
    return f"Got {len(df)} rows for AAPL"

def test_yf_download_multi():
    import yfinance as yf
    df = yf.download(["AAPL", "MSFT"], period="5d", group_by='ticker', progress=False)
    if df.empty:
        raise Exception("Empty DataFrame returned")
    return f"Got {len(df)} rows, columns: {list(df.columns)[:5]}..."

def test_yf_ticker_history():
    import yfinance as yf
    ticker = yf.Ticker("AAPL")
    hist = ticker.history(period="5d")
    if hist.empty:
        raise Exception("Empty DataFrame - this method is problematic")
    return f"Got {len(hist)} rows"

def test_yf_api_fetch():
    r = requests.post(f"{API_URL}/yahoo-finance/fetch",
                      json={"symbols": ["AAPL"], "period": "5d"},
                      timeout=60)
    if r.status_code == 200:
        data = r.json()
        return f"API returned {data.get('row_count', 0)} rows"
    raise Exception(f"Status {r.status_code}: {r.text[:500]}")

def test_yf_api_save():
    r = requests.post(f"{API_URL}/yahoo-finance/save",
                      json={"symbols": ["AAPL"], "period": "5d"},
                      timeout=60)
    if r.status_code == 200:
        data = r.json()
        return f"Saved {data.get('records_saved', 0)} records"
    raise Exception(f"Status {r.status_code}: {r.text[:500]}")

test("yfinance yf.download() single symbol", test_yf_download_single)
test("yfinance yf.download() multiple symbols", test_yf_download_multi)
test("yfinance Ticker.history() [often fails]", test_yf_ticker_history)
test("API /yahoo-finance/fetch", test_yf_api_fetch)
test("API /yahoo-finance/save", test_yf_api_save)

# =============================================================================
# TEST 4: GLEIF
# =============================================================================
print("\n" + "=" * 70)
print("TEST GROUP 4: GLEIF DATA SOURCES")
print("=" * 70)

def test_gleif_api():
    url = "https://api.gleif.org/api/v1/lei-records?page[size]=5&page[number]=1"
    r = requests.get(url, timeout=30)
    if r.status_code == 200:
        data = r.json()
        records = data.get("data", [])
        return f"Got {len(records)} records from GLEIF API"
    raise Exception(f"Status {r.status_code}")

def test_gleif_golden_copy_url1():
    url = "https://leidata.gleif.org/api/v1/concatenated-files/lei2/get/30447/zip"
    r = requests.head(url, timeout=30, allow_redirects=True)
    return f"URL1 status: {r.status_code}, Content-Type: {r.headers.get('Content-Type', 'N/A')}"

def test_gleif_golden_copy_url2():
    url = "https://goldencopy.gleif.org/api/v2/golden-copies/publishes/lei2/latest"
    r = requests.head(url, timeout=30, allow_redirects=True)
    return f"URL2 status: {r.status_code}, Final URL: {r.url[:80]}..."

test("GLEIF API (lei-records)", test_gleif_api)
test("GLEIF Golden Copy URL 1", test_gleif_golden_copy_url1)
test("GLEIF Golden Copy URL 2", test_gleif_golden_copy_url2)

# =============================================================================
# TEST 5: DUCKDB
# =============================================================================
print("\n" + "=" * 70)
print("TEST GROUP 5: DUCKDB")
print("=" * 70)

DUCKDB_PATH = os.environ.get('DUCKDB_PATH', '/app/data/warehouse.duckdb')
if not IN_DOCKER:
    DUCKDB_PATH = './data/warehouse.duckdb'

def test_duckdb_connect():
    import duckdb
    conn = duckdb.connect(DUCKDB_PATH, read_only=False)
    version = conn.execute("SELECT version()").fetchone()[0]
    conn.close()
    return f"Connected, version: {version}"

def test_duckdb_tables():
    import duckdb
    conn = duckdb.connect(DUCKDB_PATH, read_only=True)
    tables = conn.execute("SHOW TABLES").fetchall()
    table_info = []
    for (t,) in tables:
        try:
            count = conn.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
            table_info.append(f"{t}({count})")
        except:
            table_info.append(f"{t}(error)")
    conn.close()
    return f"Tables: {', '.join(table_info) if table_info else 'None'}"

def test_duckdb_write():
    import duckdb
    import pandas as pd
    conn = duckdb.connect(DUCKDB_PATH, read_only=False)
    df = pd.DataFrame({"test_col": [1, 2, 3]})
    conn.execute("DROP TABLE IF EXISTS _test_table")
    conn.execute("CREATE TABLE _test_table AS SELECT * FROM df")
    count = conn.execute("SELECT COUNT(*) FROM _test_table").fetchone()[0]
    conn.execute("DROP TABLE _test_table")
    conn.close()
    return f"Write test passed, inserted {count} rows"

test("DuckDB Connect", test_duckdb_connect)
test("DuckDB List Tables", test_duckdb_tables)
test("DuckDB Write Test", test_duckdb_write)

# =============================================================================
# TEST 6: CRON PARSER
# =============================================================================
print("\n" + "=" * 70)
print("TEST GROUP 6: CRON PARSER")
print("=" * 70)

CRON_TEST_CASES = [
    ("every hour", "0 * * * *"),
    ("daily at 5pm", "0 17 * * *"),
    ("every day at 9:30am", "30 9 * * *"),
    ("weekdays at 6pm", "0 18 * * 1-5"),
    ("every monday at 10am", "0 10 * * 1"),
    ("every 15 minutes", "*/15 * * * *"),
    ("every day except sunday at 10pm", "0 22 * * 1-6"),
]

def test_cron_parser(natural, expected):
    def _test():
        r = requests.post(f"{API_URL}/cron/parse",
                          json={"natural_language": natural},
                          timeout=30)
        if r.status_code == 200:
            data = r.json()
            actual = data.get("cron", "")
            source = data.get("source", "unknown")
            if actual == expected:
                return f"'{natural}' → '{actual}' (via {source})"
            else:
                raise Exception(f"Expected '{expected}', got '{actual}' (via {source})")
        else:
            raise Exception(f"Status {r.status_code}: {r.text[:200]}")
    return _test

for natural, expected in CRON_TEST_CASES:
    test(f"Cron: '{natural}'", test_cron_parser(natural, expected))

# =============================================================================
# TEST 7: OLLAMA
# =============================================================================
print("\n" + "=" * 70)
print("TEST GROUP 7: OLLAMA / AI")
print("=" * 70)

def test_ollama_connection():
    # Try multiple hosts
    hosts = [OLLAMA_URL, "http://ollama:11434", "http://localhost:11434", "http://host.docker.internal:11434"]
    for host in hosts:
        try:
            r = requests.get(f"{host}/api/tags", timeout=5)
            if r.status_code == 200:
                models = [m.get("name", "?") for m in r.json().get("models", [])]
                return f"Connected to {host}, models: {models if models else 'None installed'}"
        except:
            continue
    raise Exception(f"Could not connect to Ollama on any host: {hosts}")

def test_ollama_status_api():
    r = requests.get(f"{API_URL}/ollama/status", timeout=10)
    if r.status_code == 200:
        data = r.json()
        return f"Status: {data.get('status')}, Models: {data.get('models', [])}"
    raise Exception(f"Status {r.status_code}: {r.text[:200]}")

def test_ollama_generate():
    # Find working host
    hosts = [OLLAMA_URL, "http://ollama:11434", "http://localhost:11434"]
    for host in hosts:
        try:
            r = requests.post(f"{host}/api/generate",
                              json={"model": "llama3.2", "prompt": "Say 'test ok'", "stream": False},
                              timeout=60)
            if r.status_code == 200:
                response = r.json().get("response", "")[:100]
                return f"Ollama responded: {response}"
        except Exception as e:
            continue
    raise Exception("Could not generate with Ollama - model may not be installed")

def test_ai_sql():
    r = requests.post(f"{API_URL}/ai/sql",
                      json={"question": "How many tables are there?"},
                      timeout=60)
    if r.status_code == 200:
        data = r.json()
        return f"Generated SQL: {data.get('sql', 'N/A')[:100]}"
    elif r.status_code == 503:
        raise Exception("Ollama not available - AI features disabled")
    raise Exception(f"Status {r.status_code}: {r.text[:200]}")

test("Ollama Connection", test_ollama_connection)
test("API /ollama/status", test_ollama_status_api)
test("Ollama Generate (requires llama3.2)", test_ollama_generate)
test("API /ai/sql (requires Ollama)", test_ai_sql)

# =============================================================================
# TEST 8: AIRFLOW
# =============================================================================
print("\n" + "=" * 70)
print("TEST GROUP 8: AIRFLOW")
print("=" * 70)

def test_airflow_health():
    r = requests.get(f"{AIRFLOW_URL.replace('/api/v1', '')}/health", timeout=10)
    if r.status_code == 200:
        return f"Airflow healthy: {r.json()}"
    raise Exception(f"Status {r.status_code}")

def test_airflow_dags_api():
    from requests.auth import HTTPBasicAuth
    auth = HTTPBasicAuth('admin', 'admin')
    r = requests.get(f"{AIRFLOW_URL}/dags", auth=auth, timeout=10)
    if r.status_code == 200:
        dags = r.json().get("dags", [])
        dag_ids = [d.get("dag_id") for d in dags]
        return f"Found {len(dags)} DAGs: {dag_ids}"
    raise Exception(f"Status {r.status_code}: {r.text[:200]}")

def test_api_dags_list():
    r = requests.get(f"{API_URL}/dags", timeout=10)
    if r.status_code == 200:
        dags = r.json().get("dags", [])
        return f"API found {len(dags)} DAGs"
    raise Exception(f"Status {r.status_code}: {r.text[:200]}")

test("Airflow Health", test_airflow_health)
test("Airflow API /dags", test_airflow_dags_api)
test("Platform API /dags", test_api_dags_list)

# =============================================================================
# TEST 9: ICEBERG / VERSIONING
# =============================================================================
print("\n" + "=" * 70)
print("TEST GROUP 9: ICEBERG / DATA VERSIONING")
print("=" * 70)

def test_iceberg_snapshots():
    r = requests.get(f"{API_URL}/iceberg/snapshots?table_name=stock_prices", timeout=10)
    if r.status_code == 200:
        data = r.json()
        count = data.get("snapshot_count", 0)
        return f"Found {count} snapshots for stock_prices"
    raise Exception(f"Status {r.status_code}: {r.text[:200]}")

def test_iceberg_dir():
    iceberg_dir = "/app/data/iceberg/stock_prices/data" if IN_DOCKER else "./data/iceberg/stock_prices/data"
    if os.path.exists(iceberg_dir):
        files = os.listdir(iceberg_dir)
        parquet_files = [f for f in files if f.endswith('.parquet')]
        return f"Found {len(parquet_files)} parquet files in {iceberg_dir}"
    else:
        return f"Directory doesn't exist yet: {iceberg_dir}"

test("API /iceberg/snapshots", test_iceberg_snapshots)
test("Iceberg Directory Check", test_iceberg_dir)

# =============================================================================
# TEST 10: DBT TEMPLATES
# =============================================================================
print("\n" + "=" * 70)
print("TEST GROUP 10: DBT TEMPLATES")
print("=" * 70)

def test_dbt_templates_list():
    r = requests.get(f"{API_URL}/dbt/templates", timeout=10)
    if r.status_code == 200:
        templates = r.json().get("templates", [])
        names = [t.get("name") for t in templates]
        return f"Found {len(templates)} templates: {names}"
    raise Exception(f"Status {r.status_code}: {r.text[:200]}")

def test_dbt_template_detail():
    r = requests.get(f"{API_URL}/dbt/templates/daily_returns", timeout=10)
    if r.status_code == 200:
        data = r.json()
        return f"Template: {data.get('name')}, requires: {data.get('required_columns')}"
    raise Exception(f"Status {r.status_code}: {r.text[:200]}")

test("API /dbt/templates", test_dbt_templates_list)
test("API /dbt/templates/daily_returns", test_dbt_template_detail)

# =============================================================================
# TEST 11: END-TO-END PIPELINE
# =============================================================================
print("\n" + "=" * 70)
print("TEST GROUP 11: END-TO-END PIPELINE")
print("=" * 70)

def test_e2e_yahoo_to_duckdb():
    # Step 1: Fetch and save
    log("  Step 1: Fetching AAPL data via API...")
    r = requests.post(f"{API_URL}/yahoo-finance/save",
                      json={"symbols": ["AAPL"], "period": "5d"},
                      timeout=120)
    if r.status_code != 200:
        raise Exception(f"Save failed: {r.status_code} - {r.text[:200]}")

    save_result = r.json()
    log(f"  Step 1 Result: {save_result}")

    # Step 2: Query via SQL
    log("  Step 2: Querying stock_prices table...")
    r = requests.post(f"{API_URL}/query",
                      json={"query": "SELECT symbol, COUNT(*) as cnt FROM stock_prices GROUP BY symbol"},
                      timeout=30)
    if r.status_code != 200:
        raise Exception(f"Query failed: {r.status_code} - {r.text[:200]}")

    query_result = r.json()
    log(f"  Step 2 Result: {query_result}")

    # Step 3: Check Iceberg snapshots
    log("  Step 3: Checking Iceberg snapshots...")
    r = requests.get(f"{API_URL}/iceberg/snapshots?table_name=stock_prices", timeout=10)
    if r.status_code == 200:
        snapshots = r.json()
        log(f"  Step 3 Result: {snapshots.get('snapshot_count', 0)} snapshots")

    return f"Pipeline complete: saved {save_result.get('records_saved', 0)} records, found in DB"

test("E2E: Yahoo Finance → DuckDB → Query", test_e2e_yahoo_to_duckdb)

# =============================================================================
# SUMMARY
# =============================================================================
print("\n" + "=" * 70)
print("TEST SUMMARY")
print("=" * 70)

passed = RESULTS["summary"]["passed"]
failed = RESULTS["summary"]["failed"]
skipped = RESULTS["summary"]["skipped"]
total = passed + failed + skipped

print(f"Total Tests: {total}")
print(f"  ✓ Passed:  {passed}")
print(f"  ✗ Failed:  {failed}")
print(f"  ○ Skipped: {skipped}")
print()

if failed > 0:
    print("FAILED TESTS:")
    print("-" * 40)
    for t in RESULTS["tests"]:
        if t["status"] == "FAILED":
            print(f"  • {t['name']}")
            print(f"    Error: {t['error']}")
            if t.get('traceback'):
                # Print first 3 lines of traceback
                tb_lines = t['traceback'].strip().split('\n')[-3:]
                for line in tb_lines:
                    print(f"    {line}")
            print()

# Write JSON results for detailed analysis
print("\n" + "=" * 70)
print("DETAILED RESULTS (JSON)")
print("=" * 70)
print(json.dumps(RESULTS, indent=2, default=str))

print("\n" + "=" * 70)
print("NEXT STEPS")
print("=" * 70)
print("""
1. Copy everything above and share with Claude
2. Or save to file: python test_all.py > test_results.log 2>&1

Common fixes based on failures:
- Yahoo Finance fails: Upgrade yfinance, rebuild container
- GLEIF fails: Check internet connectivity, URLs may have changed
- Ollama fails: Run 'docker exec ollama ollama pull llama3.2'
- DuckDB fails: Check file permissions on ./data directory
- Airflow fails: Wait for containers to fully start (~60s)
""")
