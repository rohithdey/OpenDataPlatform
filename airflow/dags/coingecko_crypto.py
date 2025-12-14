"""
CoinGecko Cryptocurrency Data Pipeline DAG
Fetches cryptocurrency market data from CoinGecko's free API.

Free tier available - no API key required for basic access.
Rate limit: 10-30 calls/minute on free tier.

Data includes:
- Price, market cap, volume
- 24h/7d/30d price changes
- All-time high/low
- Circulating/total supply
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os
import pandas as pd

# Configuration
DATA_DIR = '/opt/airflow/data'
DUCKDB_PATH = '/opt/airflow/data/warehouse.duckdb'
DELTA_TABLE_PATH = '/opt/airflow/data/delta/crypto_prices'

# Default cryptocurrencies to track
DEFAULT_COINS = [
    'bitcoin',
    'ethereum',
    'tether',
    'binancecoin',
    'solana',
    'ripple',
    'cardano',
    'dogecoin',
    'polkadot',
    'avalanche-2',
    'chainlink',
    'polygon',
    'litecoin',
    'uniswap',
]

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}


def fetch_coingecko_data(**context):
    """
    Fetch cryptocurrency data from CoinGecko's free API.
    No API key required for basic access.
    """
    import requests
    import time

    dag_run = context.get('dag_run')
    conf = dag_run.conf if dag_run and dag_run.conf else {}

    coins = conf.get('coins', DEFAULT_COINS)
    currency = conf.get('currency', 'usd')

    if isinstance(coins, str):
        coins = [c.strip().lower() for c in coins.split(',')]

    print(f"[CoinGecko] Fetching data for {len(coins)} coins")

    # CoinGecko API endpoint for market data
    url = "https://api.coingecko.com/api/v3/coins/markets"

    params = {
        'vs_currency': currency,
        'ids': ','.join(coins),
        'order': 'market_cap_desc',
        'per_page': 100,
        'page': 1,
        'sparkline': 'false',
        'price_change_percentage': '1h,24h,7d,30d'
    }

    headers = {
        'Accept': 'application/json',
    }

    all_data = []

    try:
        response = requests.get(url, params=params, headers=headers, timeout=30)

        if response.status_code == 200:
            data = response.json()

            for coin in data:
                all_data.append({
                    'coin_id': coin.get('id', ''),
                    'symbol': coin.get('symbol', '').upper(),
                    'name': coin.get('name', ''),
                    'current_price': coin.get('current_price'),
                    'market_cap': coin.get('market_cap'),
                    'market_cap_rank': coin.get('market_cap_rank'),
                    'total_volume': coin.get('total_volume'),
                    'high_24h': coin.get('high_24h'),
                    'low_24h': coin.get('low_24h'),
                    'price_change_24h': coin.get('price_change_24h'),
                    'price_change_pct_24h': coin.get('price_change_percentage_24h'),
                    'price_change_pct_7d': coin.get('price_change_percentage_7d_in_currency'),
                    'price_change_pct_30d': coin.get('price_change_percentage_30d_in_currency'),
                    'circulating_supply': coin.get('circulating_supply'),
                    'total_supply': coin.get('total_supply'),
                    'max_supply': coin.get('max_supply'),
                    'ath': coin.get('ath'),  # All-time high
                    'ath_date': coin.get('ath_date'),
                    'ath_change_pct': coin.get('ath_change_percentage'),
                    'atl': coin.get('atl'),  # All-time low
                    'atl_date': coin.get('atl_date'),
                    'last_updated': coin.get('last_updated'),
                    'currency': currency,
                })

            print(f"[CoinGecko] Got data for {len(all_data)} coins")

        elif response.status_code == 429:
            print("[CoinGecko] Rate limited. Waiting before retry...")
            time.sleep(60)
            raise Exception("Rate limited by CoinGecko")
        else:
            print(f"[CoinGecko] Error: {response.status_code} - {response.text}")

    except Exception as e:
        print(f"[CoinGecko] Error: {e}")
        raise

    if not all_data:
        raise ValueError("No data fetched from CoinGecko")

    df = pd.DataFrame(all_data)
    df['fetch_timestamp'] = datetime.now().isoformat()
    df['fetch_date'] = datetime.now().date()

    print(f"[CoinGecko] Total records: {len(df)}")

    return df.to_json(orient='records', date_format='iso')


def fetch_historical_prices(**context):
    """
    Fetch historical price data for top coins.
    This provides OHLC data for charting.
    """
    import requests
    import time

    dag_run = context.get('dag_run')
    conf = dag_run.conf if dag_run and dag_run.conf else {}

    # Only fetch historical for top coins to avoid rate limits
    coins = conf.get('historical_coins', ['bitcoin', 'ethereum', 'solana'])
    days = conf.get('days', 30)
    currency = conf.get('currency', 'usd')

    if isinstance(coins, str):
        coins = [c.strip().lower() for c in coins.split(',')]

    print(f"[CoinGecko] Fetching {days}-day history for {coins}")

    all_history = []

    for coin_id in coins[:5]:  # Limit to 5 coins to avoid rate limits
        try:
            url = f"https://api.coingecko.com/api/v3/coins/{coin_id}/ohlc"
            params = {
                'vs_currency': currency,
                'days': days
            }

            response = requests.get(url, params=params, timeout=30)

            if response.status_code == 200:
                data = response.json()

                for ohlc in data:
                    # OHLC format: [timestamp, open, high, low, close]
                    if len(ohlc) >= 5:
                        all_history.append({
                            'coin_id': coin_id,
                            'timestamp': datetime.fromtimestamp(ohlc[0] / 1000),
                            'open': ohlc[1],
                            'high': ohlc[2],
                            'low': ohlc[3],
                            'close': ohlc[4],
                            'currency': currency,
                        })

                print(f"[CoinGecko] Got {len([h for h in all_history if h['coin_id'] == coin_id])} OHLC points for {coin_id}")

            time.sleep(2)  # Rate limiting

        except Exception as e:
            print(f"[CoinGecko] Error fetching history for {coin_id}: {e}")

    if all_history:
        return pd.DataFrame(all_history).to_json(orient='records', date_format='iso')
    return None


def save_to_delta_lake(**context):
    """Save crypto data to Delta Lake."""
    from deltalake import write_deltalake, DeltaTable
    import pyarrow as pa
    from io import StringIO

    ti = context['ti']
    json_data = ti.xcom_pull(task_ids='fetch_coingecko_data')

    if not json_data:
        raise ValueError("No data received from fetch task")

    df = pd.read_json(StringIO(json_data), orient='records')
    print(f"[Delta Lake] Processing {len(df)} records")

    os.makedirs(os.path.dirname(DELTA_TABLE_PATH), exist_ok=True)

    df['ingestion_date'] = datetime.now().date()

    # Convert date columns
    for col in ['ath_date', 'atl_date', 'last_updated']:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors='coerce')

    # Convert to PyArrow Table for Delta Lake compatibility
    table = pa.Table.from_pandas(df)

    try:
        delta_log_path = os.path.join(DELTA_TABLE_PATH, '_delta_log')
        if os.path.exists(DELTA_TABLE_PATH) and os.path.exists(delta_log_path):
            write_deltalake(DELTA_TABLE_PATH, table, mode="append", schema_mode="merge")
        else:
            write_deltalake(DELTA_TABLE_PATH, table, mode="overwrite", partition_by=["symbol"])

        dt = DeltaTable(DELTA_TABLE_PATH)
        print(f"[Delta Lake] Saved. Version: {dt.version()}")

        return {'path': DELTA_TABLE_PATH, 'version': dt.version(), 'records': len(df)}

    except Exception as e:
        print(f"[Delta Lake] Error: {e}")
        raise


def save_ohlc_to_delta(**context):
    """Save OHLC historical data to separate Delta table."""
    from deltalake import write_deltalake, DeltaTable
    import pyarrow as pa
    from io import StringIO

    ti = context['ti']
    json_data = ti.xcom_pull(task_ids='fetch_historical_prices')

    if not json_data:
        print("[Delta Lake] No OHLC data to save")
        return

    df = pd.read_json(StringIO(json_data), orient='records')
    ohlc_path = '/opt/airflow/data/delta/crypto_ohlc'

    os.makedirs(os.path.dirname(ohlc_path), exist_ok=True)

    df['timestamp'] = pd.to_datetime(df['timestamp'])

    # Convert to PyArrow Table for Delta Lake compatibility
    table = pa.Table.from_pandas(df)

    try:
        delta_log_path = os.path.join(ohlc_path, '_delta_log')
        if os.path.exists(ohlc_path) and os.path.exists(delta_log_path):
            write_deltalake(ohlc_path, table, mode="append", schema_mode="merge")
        else:
            write_deltalake(ohlc_path, table, mode="overwrite", partition_by=["coin_id"])

        dt = DeltaTable(ohlc_path)
        print(f"[Delta Lake] OHLC saved. Version: {dt.version()}")

    except Exception as e:
        print(f"[Delta Lake] OHLC error: {e}")


def sync_to_duckdb(**context):
    """Sync to DuckDB."""
    import duckdb

    conn = duckdb.connect(DUCKDB_PATH, read_only=False)

    # Sync main crypto prices
    try:
        conn.execute("INSTALL delta;")
        conn.execute("LOAD delta;")
        conn.execute(f"""
            CREATE OR REPLACE TABLE crypto_prices AS
            SELECT * FROM delta_scan('{DELTA_TABLE_PATH}')
        """)
    except:
        conn.execute(f"""
            CREATE OR REPLACE TABLE crypto_prices AS
            SELECT * FROM read_parquet('{DELTA_TABLE_PATH}/*.parquet')
        """)

    count = conn.execute("SELECT COUNT(*) FROM crypto_prices").fetchone()[0]
    print(f"[DuckDB] Synced {count} crypto price records")

    # Try to sync OHLC data if it exists
    ohlc_path = '/opt/airflow/data/delta/crypto_ohlc'
    if os.path.exists(ohlc_path):
        try:
            conn.execute(f"""
                CREATE OR REPLACE TABLE crypto_ohlc AS
                SELECT * FROM delta_scan('{ohlc_path}')
            """)
            ohlc_count = conn.execute("SELECT COUNT(*) FROM crypto_ohlc").fetchone()[0]
            print(f"[DuckDB] Synced {ohlc_count} OHLC records")
        except:
            pass

    conn.close()


def create_crypto_views(**context):
    """Create useful views for crypto analysis."""
    import duckdb

    conn = duckdb.connect(DUCKDB_PATH, read_only=False)

    # Latest prices with rankings
    conn.execute("""
        CREATE OR REPLACE VIEW crypto_latest AS
        SELECT
            symbol,
            name,
            current_price,
            market_cap,
            market_cap_rank,
            total_volume,
            price_change_pct_24h,
            price_change_pct_7d,
            ath,
            ath_change_pct,
            fetch_date
        FROM crypto_prices
        WHERE (symbol, fetch_timestamp) IN (
            SELECT symbol, MAX(fetch_timestamp)
            FROM crypto_prices
            GROUP BY symbol
        )
        ORDER BY market_cap_rank
    """)

    # Top gainers/losers
    conn.execute("""
        CREATE OR REPLACE VIEW crypto_movers AS
        SELECT
            symbol,
            name,
            current_price,
            price_change_pct_24h,
            price_change_pct_7d,
            CASE
                WHEN price_change_pct_24h > 0 THEN 'gainer'
                ELSE 'loser'
            END as direction
        FROM crypto_prices
        WHERE (symbol, fetch_timestamp) IN (
            SELECT symbol, MAX(fetch_timestamp)
            FROM crypto_prices
            GROUP BY symbol
        )
        ORDER BY ABS(price_change_pct_24h) DESC
    """)

    # Distance from ATH
    conn.execute("""
        CREATE OR REPLACE VIEW crypto_ath_distance AS
        SELECT
            symbol,
            name,
            current_price,
            ath,
            ROUND(ath_change_pct, 2) as pct_from_ath,
            ath_date
        FROM crypto_prices
        WHERE (symbol, fetch_timestamp) IN (
            SELECT symbol, MAX(fetch_timestamp)
            FROM crypto_prices
            GROUP BY symbol
        )
        ORDER BY ath_change_pct DESC
    """)

    # Market summary
    conn.execute("""
        CREATE OR REPLACE VIEW crypto_market_summary AS
        SELECT
            fetch_date,
            COUNT(DISTINCT symbol) as coins_tracked,
            SUM(market_cap) as total_market_cap,
            SUM(total_volume) as total_volume_24h,
            AVG(price_change_pct_24h) as avg_24h_change
        FROM crypto_prices
        WHERE (symbol, fetch_timestamp) IN (
            SELECT symbol, MAX(fetch_timestamp)
            FROM crypto_prices
            GROUP BY symbol
        )
        GROUP BY fetch_date
        ORDER BY fetch_date DESC
    """)

    print("[DuckDB] Created views: crypto_latest, crypto_movers, crypto_ath_distance, crypto_market_summary")
    conn.close()


# DAG Definition
with DAG(
    'coingecko_crypto_hourly',
    default_args=default_args,
    description='Fetch cryptocurrency data from CoinGecko',
    schedule_interval='0 * * * *',  # Every hour
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['crypto', 'coingecko', 'bitcoin', 'ethereum', 'delta-lake'],
) as dag:

    dag.doc_md = """
    ## CoinGecko Cryptocurrency Pipeline

    Fetches cryptocurrency market data from CoinGecko's free API.

    ### Default Coins:
    Bitcoin, Ethereum, Solana, Cardano, Dogecoin, Polkadot, Chainlink, Polygon, etc.

    ### Data Includes:
    - Current price, market cap, volume
    - 24h/7d/30d price changes
    - All-time high/low and distance from ATH
    - Supply metrics

    ### Trigger with custom coins:
    ```json
    {
        "coins": "bitcoin,ethereum,solana,cardano",
        "currency": "usd"
    }
    ```

    ### Rate Limits:
    Free tier: 10-30 calls/minute
    Consider reducing frequency or getting API key for heavy use.
    """

    fetch_task = PythonOperator(
        task_id='fetch_coingecko_data',
        python_callable=fetch_coingecko_data,
        provide_context=True,
    )

    fetch_history_task = PythonOperator(
        task_id='fetch_historical_prices',
        python_callable=fetch_historical_prices,
        provide_context=True,
    )

    save_task = PythonOperator(
        task_id='save_to_delta',
        python_callable=save_to_delta_lake,
        provide_context=True,
    )

    save_ohlc_task = PythonOperator(
        task_id='save_ohlc_to_delta',
        python_callable=save_ohlc_to_delta,
        provide_context=True,
    )

    sync_task = PythonOperator(
        task_id='sync_to_duckdb',
        python_callable=sync_to_duckdb,
        provide_context=True,
    )

    views_task = PythonOperator(
        task_id='create_views',
        python_callable=create_crypto_views,
        provide_context=True,
    )

    fetch_task >> save_task >> sync_task >> views_task
    fetch_history_task >> save_ohlc_task >> sync_task
