#!/usr/bin/env python3
"""
Diagnostic script for Open Data Platform
Run this inside the Airflow container to test data fetching:

    docker exec -it opendataplatform-airflow-scheduler-1 python /opt/airflow/dags/test_data_sources.py
"""

import sys
print(f"Python version: {sys.version}")

# ============== TEST 1: Yahoo Finance ==============
print("\n" + "="*60)
print("TEST 1: Yahoo Finance")
print("="*60)

try:
    import yfinance as yf
    print(f"yfinance version: {yf.__version__}")

    # Test 1a: Simple download
    print("\n[Test 1a] Testing yf.download() for AAPL...")
    df = yf.download("AAPL", period="5d", progress=False)

    if df.empty:
        print("FAILED: yf.download() returned empty DataFrame")
    else:
        print(f"SUCCESS: Got {len(df)} rows")
        print(df.head())

    # Test 1b: Multiple symbols
    print("\n[Test 1b] Testing multiple symbols (AAPL, MSFT)...")
    df2 = yf.download(["AAPL", "MSFT"], period="5d", group_by='ticker', progress=False)

    if df2.empty:
        print("FAILED: Multi-symbol download returned empty DataFrame")
    else:
        print(f"SUCCESS: Got {len(df2)} rows")
        print(f"Columns: {list(df2.columns)[:10]}")

    # Test 1c: Ticker method (the one that was failing)
    print("\n[Test 1c] Testing Ticker().history() for AAPL...")
    ticker = yf.Ticker("AAPL")
    hist = ticker.history(period="5d")

    if hist.empty:
        print("FAILED: Ticker.history() returned empty DataFrame")
        print("This is the method that was failing in the DAG")
    else:
        print(f"SUCCESS: Got {len(hist)} rows")

except ImportError as e:
    print(f"FAILED: yfinance not installed - {e}")
except Exception as e:
    print(f"FAILED: {type(e).__name__}: {e}")

# ============== TEST 2: GLEIF API ==============
print("\n" + "="*60)
print("TEST 2: GLEIF API")
print("="*60)

try:
    import requests

    # Test 2a: API endpoint
    print("\n[Test 2a] Testing GLEIF API (small request)...")
    api_url = "https://api.gleif.org/api/v1/lei-records?page[size]=5&page[number]=1"
    resp = requests.get(api_url, timeout=30)

    if resp.status_code == 200:
        data = resp.json()
        records = data.get("data", [])
        print(f"SUCCESS: API returned {len(records)} records")
        if records:
            first = records[0].get("attributes", {})
            print(f"Sample LEI: {first.get('lei', 'N/A')}")
    else:
        print(f"FAILED: API returned status {resp.status_code}")
        print(resp.text[:500])

except Exception as e:
    print(f"FAILED: {type(e).__name__}: {e}")

# ============== TEST 3: GLEIF Golden Copy ==============
print("\n" + "="*60)
print("TEST 3: GLEIF Golden Copy Download")
print("="*60)

try:
    import requests

    # Test 3a: Check if URL is accessible
    print("\n[Test 3a] Testing Golden Copy URL (HEAD request)...")
    golden_url = "https://goldencopy.gleif.org/api/v2/golden-copies/publishes/lei2/latest.csv"

    resp = requests.head(golden_url, allow_redirects=True, timeout=30)
    print(f"Status: {resp.status_code}")
    print(f"Final URL: {resp.url}")
    print(f"Content-Type: {resp.headers.get('Content-Type', 'N/A')}")
    print(f"Content-Length: {resp.headers.get('Content-Length', 'N/A')}")

    if resp.status_code != 200:
        print(f"ISSUE: URL returned {resp.status_code}")

        # Try alternative URL
        print("\n[Test 3b] Trying alternative Golden Copy URLs...")
        alt_urls = [
            "https://leidata.gleif.org/api/v2/golden-copies/publishes/lei2/latest",
            "https://leidata-preview.gleif.org/api/v1/leifiles/latest"
        ]
        for url in alt_urls:
            try:
                r = requests.head(url, allow_redirects=True, timeout=10)
                print(f"  {url} -> {r.status_code}")
            except Exception as e:
                print(f"  {url} -> ERROR: {e}")

except Exception as e:
    print(f"FAILED: {type(e).__name__}: {e}")

# ============== TEST 4: DuckDB ==============
print("\n" + "="*60)
print("TEST 4: DuckDB")
print("="*60)

try:
    import duckdb
    print(f"DuckDB version: {duckdb.__version__}")

    # Test connection
    print("\n[Test 4a] Testing DuckDB connection...")
    db_path = '/opt/airflow/data/warehouse.duckdb'
    conn = duckdb.connect(db_path, read_only=False)

    # List tables
    tables = conn.execute("SHOW TABLES").fetchall()
    print(f"Tables in database: {[t[0] for t in tables]}")

    # Check for data
    for table in tables:
        count = conn.execute(f"SELECT COUNT(*) FROM {table[0]}").fetchone()[0]
        print(f"  {table[0]}: {count:,} rows")

    conn.close()
    print("SUCCESS: DuckDB connection works")

except Exception as e:
    print(f"FAILED: {type(e).__name__}: {e}")

# ============== TEST 5: Ollama ==============
print("\n" + "="*60)
print("TEST 5: Ollama")
print("="*60)

try:
    import requests

    # Try different hosts
    hosts = [
        "http://ollama:11434",
        "http://host.docker.internal:11434",
        "http://localhost:11434"
    ]

    for host in hosts:
        print(f"\n[Testing] {host}...")
        try:
            resp = requests.get(f"{host}/api/tags", timeout=5)
            if resp.status_code == 200:
                models = resp.json().get("models", [])
                model_names = [m.get("name", "unknown") for m in models]
                print(f"SUCCESS: Connected! Models: {model_names or 'None installed'}")

                if not model_names:
                    print("  To install: docker exec ollama ollama pull llama3.2")
                break
            else:
                print(f"  Status: {resp.status_code}")
        except requests.exceptions.ConnectionError:
            print(f"  Connection refused")
        except Exception as e:
            print(f"  Error: {e}")
    else:
        print("\nFAILED: Could not connect to Ollama on any host")

except Exception as e:
    print(f"FAILED: {type(e).__name__}: {e}")

# ============== SUMMARY ==============
print("\n" + "="*60)
print("DIAGNOSTIC COMPLETE")
print("="*60)
print("""
If Yahoo Finance fails:
  - Try: pip install --upgrade yfinance
  - Or in Dockerfile: change yfinance==0.2.36 to yfinance>=0.2.40

If GLEIF Golden Copy fails:
  - URL may have changed - check https://www.gleif.org/en/lei-data/gleif-golden-copy
  - Try the GLEIF API DAG instead (smaller dataset)

If Ollama not connected:
  - Check: docker compose logs ollama
  - Pull model: docker exec opendataplatform-ollama-1 ollama pull llama3.2
""")
